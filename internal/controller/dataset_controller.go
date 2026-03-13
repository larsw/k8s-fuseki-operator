package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type DatasetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securitypolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch

func (r *DatasetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var dataset fusekiv1alpha1.Dataset
	if err := r.Get(ctx, req.NamespacedName, &dataset); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	missingPolicyRefs, err := r.reconcileConfigMap(ctx, &dataset)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := dataset.DeepCopy()
	updated.Status.ObservedGeneration = dataset.Generation
	updated.Status.ConfigMapName = dataset.ConfigMapName()
	updated.Status.Phase = "Defined"
	condition := metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "ConfigRendered",
		Message:            "Dataset config is reconciled.",
		ObservedGeneration: dataset.Generation,
	}
	if len(missingPolicyRefs) > 0 {
		updated.Status.Phase = "Pending"
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SecurityPoliciesMissing"
		condition.Message = "Waiting for referenced SecurityPolicies: " + strings.Join(missingPolicyRefs, ", ")
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, condition)

	if !reflect.DeepEqual(dataset.Status, updated.Status) {
		dataset.Status = updated.Status
		if err := r.Status().Update(ctx, &dataset); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *DatasetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.Dataset{}).
		Watches(&fusekiv1alpha1.SecurityPolicy{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecurityPolicy)).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func (r *DatasetReconciler) reconcileConfigMap(ctx context.Context, dataset *fusekiv1alpha1.Dataset) ([]string, error) {
	policies, missingPolicyRefs, err := resolveDatasetSecurityPolicies(ctx, r.Client, dataset)
	if err != nil {
		return nil, err
	}

	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: dataset.ConfigMapName(), Namespace: dataset.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = mergeStringMaps(datasetLabels(dataset), map[string]string{"fuseki.apache.org/component": "dataset-config"})
		configMap.Data = map[string]string{
			"dataset.properties": renderDatasetProperties(dataset),
			"preload.txt":        strings.Join(preloadEntries(dataset.Spec.Preload), "\n"),
		}
		if len(missingPolicyRefs) == 0 {
			delete(configMap.Data, "security-policies.missing")
			bundle, err := renderDatasetSecurityPolicyBundle(policies)
			if err != nil {
				return err
			}
			configMap.Data["security-policies.json"] = bundle
		} else {
			delete(configMap.Data, "security-policies.json")
			configMap.Data["security-policies.missing"] = strings.Join(missingPolicyRefs, "\n") + "\n"
		}
		if dataset.Spec.Spatial != nil && dataset.Spec.Spatial.Enabled {
			configMap.Data["spatial.properties"] = renderSpatialProperties(dataset)
		}

		return controllerutil.SetControllerReference(dataset, configMap, r.Scheme)
	})
	return missingPolicyRefs, err
}

func (r *DatasetReconciler) requestsForSecurityPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	var datasets fusekiv1alpha1.DatasetList
	if err := r.List(ctx, &datasets, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range datasets.Items {
		dataset := &datasets.Items[i]
		if !datasetReferencesSecurityPolicy(dataset, obj.GetName()) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(dataset)})
	}
	return requests
}

func datasetLabels(dataset *fusekiv1alpha1.Dataset) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":         "fuseki-dataset",
		"app.kubernetes.io/instance":     dataset.Name,
		"app.kubernetes.io/managed-by":   "fuseki-operator",
		"fuseki.apache.org/dataset":      dataset.Name,
		"fuseki.apache.org/dataset-name": dataset.Spec.Name,
	}
}

func renderDatasetProperties(dataset *fusekiv1alpha1.Dataset) string {
	properties := []string{
		"dataset.name=" + dataset.Spec.Name,
		"dataset.type=" + string(dataset.DesiredType()),
	}
	if dataset.Spec.DisplayName != "" {
		properties = append(properties, "dataset.displayName="+dataset.Spec.DisplayName)
	}
	if dataset.Spec.Spatial != nil {
		properties = append(properties, fmt.Sprintf("spatial.enabled=%t", dataset.Spec.Spatial.Enabled))
	}
	return strings.Join(properties, "\n") + "\n"
}

func renderSpatialProperties(dataset *fusekiv1alpha1.Dataset) string {
	spatial := dataset.Spec.Spatial
	properties := []string{
		"spatial.enabled=true",
		"spatial.indexPath=" + dataset.DesiredSpatialIndexPath(),
	}
	if spatial == nil {
		return strings.Join(properties, "\n") + "\n"
	}
	if spatial.Assembler != "" {
		properties = append(properties, "spatial.assembler="+spatial.Assembler)
	}
	if spatial.AdditionalClasses != "" {
		properties = append(properties, "spatial.additionalClasses="+spatial.AdditionalClasses)
	}
	return strings.Join(properties, "\n") + "\n"
}

func preloadEntries(sources []fusekiv1alpha1.DatasetPreloadSource) []string {
	entries := make([]string, 0, len(sources))
	for _, source := range sources {
		entries = append(entries, source.URI+"|"+source.Format)
	}
	return entries
}

func reconcileDatasetBootstrapJobs(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, target datasetBootstrapTarget, datasetRefs []corev1.LocalObjectReference, ownerLabels map[string]string) error {
	securityProfile, adminSecretRef, err := resolveDatasetSecurity(ctx, c, owner.GetNamespace(), target.SecurityProfileRef)
	if err != nil {
		return err
	}
	target.WriteURL = secureServiceURL(target.WriteURL, securityProfile)

	for _, ref := range datasetRefs {
		if ref.Name == "" {
			continue
		}

		var dataset fusekiv1alpha1.Dataset
		if err := c.Get(ctx, client.ObjectKey{Namespace: owner.GetNamespace(), Name: ref.Name}, &dataset); err != nil {
			return fmt.Errorf("get dataset %s for %s %s: %w", ref.Name, target.Kind, target.Name, err)
		}

		if err := reconcileDatasetBootstrapJob(ctx, c, scheme, owner, &dataset, target, ownerLabels, securityProfile, adminSecretRef); err != nil {
			return err
		}
	}

	return nil
}

func reconcileDatasetBootstrapJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, dataset *fusekiv1alpha1.Dataset, target datasetBootstrapTarget, ownerLabels map[string]string, securityProfile *fusekiv1alpha1.SecurityProfile, adminSecretRef *corev1.LocalObjectReference) error {
	jobName := datasetBootstrapJobName(target, dataset.Name)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: owner.GetNamespace()}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, job, func() error {
		desiredLabels := mergeStringMaps(ownerLabels, map[string]string{
			"fuseki.apache.org/component":    "dataset-bootstrap",
			"fuseki.apache.org/dataset":      dataset.Name,
			"fuseki.apache.org/dataset-name": dataset.Spec.Name,
		})
		job.Labels = desiredLabels
		if job.CreationTimestamp.IsZero() {
			job.Spec.BackoffLimit = ptrTo(int32(3))
			job.Spec.Template.ObjectMeta.Labels = mergeStringMaps(desiredLabels, map[string]string{"job-name": jobName})
			job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
			job.Spec.Template.Spec.Containers = []corev1.Container{datasetBootstrapContainer(dataset, target, securityProfile, adminSecretRef)}
			volumes := []corev1.Volume{{
				Name:         datasetConfigVolumeName,
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: dataset.ConfigMapName()}}},
			}}
			if securityVolume := fusekiSecurityConfigVolume(securityProfile); securityVolume != nil {
				volumes = append(volumes, *securityVolume)
			}
			if tlsVolume := fusekiSecurityTLSVolume(securityProfile); tlsVolume != nil {
				volumes = append(volumes, *tlsVolume)
			}
			job.Spec.Template.Spec.Volumes = volumes
		}
		return controllerutil.SetControllerReference(owner, job, scheme)
	})
	return err
}

func datasetBootstrapJobName(target datasetBootstrapTarget, datasetName string) string {
	return fmt.Sprintf("%s-%s-%s-bootstrap", target.Kind, target.Name, datasetName)
}

func datasetBootstrapContainer(dataset *fusekiv1alpha1.Dataset, target datasetBootstrapTarget, securityProfile *fusekiv1alpha1.SecurityProfile, adminSecretRef *corev1.LocalObjectReference) corev1.Container {
	script := strings.Join([]string{
		"set -eu",
		"curl_fuseki() {",
		"  if [ -n \"${SECURITY_PROFILE_TLS_CA_FILE:-}\" ] && [ -f \"${SECURITY_PROFILE_TLS_CA_FILE}\" ]; then",
		"    curl --cacert \"${SECURITY_PROFILE_TLS_CA_FILE}\" \"$@\"",
		"    return",
		"  fi",
		"  if [ -n \"${SECURITY_PROFILE_TLS_CERT_FILE:-}\" ] && [ -f \"${SECURITY_PROFILE_TLS_CERT_FILE}\" ]; then",
		"    curl --cacert \"${SECURITY_PROFILE_TLS_CERT_FILE}\" \"$@\"",
		"    return",
		"  fi",
		"  if [ \"${FUSEKI_WRITE_URL#https://}\" != \"${FUSEKI_WRITE_URL}\" ]; then",
		"    curl -k \"$@\"",
		"    return",
		"  fi",
		"  curl \"$@\"",
		"}",
		"echo \"Bootstrapping dataset ${DATASET_NAME} via ${FUSEKI_WRITE_URL}\"",
		"cat /dataset-config/dataset.properties",
		"if [ -f /dataset-config/spatial.properties ]; then cat /dataset-config/spatial.properties; fi",
		"for attempt in $(seq 1 60); do",
		"  if curl_fuseki --silent --fail \"${FUSEKI_WRITE_URL}/$/ping\" >/dev/null 2>&1; then",
		"    break",
		"  fi",
		"  if [ \"${attempt}\" -eq 60 ]; then",
		"    echo \"Fuseki write service did not become ready in time\" >&2",
		"    exit 1",
		"  fi",
		"  sleep 2",
		"done",
		"if [ -n \"${FUSEKI_ADMIN_USER:-}\" ] && [ -n \"${FUSEKI_ADMIN_PASSWORD:-}\" ]; then",
		"  status=$(curl_fuseki --silent --output /tmp/fuseki-create.out --write-out '%{http_code}' -u \"${FUSEKI_ADMIN_USER}:${FUSEKI_ADMIN_PASSWORD}\" -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' --data \"dbName=${DATASET_NAME}&dbType=${DATASET_DB_TYPE}\" \"${FUSEKI_WRITE_URL}/$/datasets\")",
		"  if [ \"${status}\" != \"200\" ] && [ \"${status}\" != \"201\" ] && [ \"${status}\" != \"409\" ]; then cat /tmp/fuseki-create.out; exit 1; fi",
		"else",
		"  echo \"Admin credentials are not configured; skipping authenticated dataset creation.\"",
		"fi",
		"if [ -s /dataset-config/preload.txt ] && [ -n \"${FUSEKI_ADMIN_USER:-}\" ] && [ -n \"${FUSEKI_ADMIN_PASSWORD:-}\" ]; then",
		"  while IFS='|' read -r uri format; do",
		"    [ -z \"${uri}\" ] && continue",
		"    content_type=${format:-text/turtle}",
		"    curl --silent --show-error --fail -L \"${uri}\" | curl_fuseki --silent --show-error --fail -u \"${FUSEKI_ADMIN_USER}:${FUSEKI_ADMIN_PASSWORD}\" -X POST -H \"Content-Type: ${content_type}\" --data-binary @- \"${FUSEKI_WRITE_URL}/${DATASET_NAME}/data\"",
		"  done < /dataset-config/preload.txt",
		"elif [ -s /dataset-config/preload.txt ]; then",
		"  echo \"Preload sources configured but admin credentials are absent; skipping data load.\"",
		"fi",
	}, "\n")

	env := []corev1.EnvVar{
		{Name: "DATASET_NAME", Value: dataset.Spec.Name},
		{Name: "DATASET_DB_TYPE", Value: strings.ToLower(string(dataset.DesiredType()))},
		{Name: "FUSEKI_WRITE_URL", Value: target.WriteURL},
	}
	env = append(env, fusekiSecurityEnvVars(securityProfile)...)
	if adminSecretRef != nil {
		env = append(env,
			corev1.EnvVar{Name: "FUSEKI_ADMIN_USER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *adminSecretRef, Key: "username", Optional: ptrTo(true)}}},
			corev1.EnvVar{Name: "FUSEKI_ADMIN_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *adminSecretRef, Key: "password", Optional: ptrTo(true)}}},
		)
	}

	return corev1.Container{
		Name:            "bootstrap",
		Image:           target.Image,
		ImagePullPolicy: target.ImagePullPolicy,
		Command:         []string{"/bin/sh", "-ceu", script},
		Env:             env,
		VolumeMounts:    datasetBootstrapVolumeMounts(securityProfile),
	}
}

func datasetBootstrapVolumeMounts(securityProfile *fusekiv1alpha1.SecurityProfile) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{{
		Name:      datasetConfigVolumeName,
		MountPath: "/dataset-config",
		ReadOnly:  true,
	}}
	if securityMount := fusekiSecurityConfigVolumeMount(securityProfile); securityMount != nil {
		mounts = append(mounts, *securityMount)
	}
	if tlsMount := fusekiSecurityTLSVolumeMount(securityProfile); tlsMount != nil {
		mounts = append(mounts, *tlsMount)
	}
	return mounts
}

func resolveDatasetSecurity(ctx context.Context, c client.Client, namespace string, securityProfileRef *corev1.LocalObjectReference) (*fusekiv1alpha1.SecurityProfile, *corev1.LocalObjectReference, error) {
	if securityProfileRef == nil || securityProfileRef.Name == "" {
		return nil, nil, nil
	}

	var profile fusekiv1alpha1.SecurityProfile
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: securityProfileRef.Name}, &profile); err != nil {
		return nil, nil, err
	}

	return &profile, profile.Spec.AdminCredentialsSecretRef, nil
}
