package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

const rdfDeltaDataVolumeName = "data"

type RDFDeltaServerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=backuppolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

func (r *RDFDeltaServerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server fusekiv1alpha1.RDFDeltaServer
	if err := r.Get(ctx, req.NamespacedName, &server); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	activeRestore, err := resolveActiveRestoreRequest(ctx, r.Client, server.Namespace, server.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	backupStatus, err := resolveBackupPolicyDependency(ctx, r.Client, server.Namespace, server.Spec.BackupPolicyRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileConfigMap(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}

	statefulSet, err := r.reconcileStatefulSet(ctx, &server, activeRestore != nil)
	if err != nil {
		return ctrl.Result{}, err
	}

	backupConditionReason := backupStatus.Reason
	backupConditionMessage := backupStatus.Message
	backupConditionStatus := backupStatus.Status
	if server.Spec.BackupPolicyRef == nil {
		if err := deleteRDFDeltaBackupCronJob(ctx, r.Client, server.Namespace, server.BackupCronJobName()); err != nil {
			return ctrl.Result{}, err
		}
	} else if backupStatus.Status != metav1.ConditionTrue || backupStatus.Policy == nil {
		if err := deleteRDFDeltaBackupCronJob(ctx, r.Client, server.Namespace, server.BackupCronJobName()); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		if err := r.reconcileBackupCronJob(ctx, &server, backupStatus.Policy); err != nil {
			return ctrl.Result{}, err
		}
		backupConditionStatus = metav1.ConditionTrue
		backupConditionReason = "BackupCronJobReady"
		backupConditionMessage = fmt.Sprintf("Backup CronJob %q is reconciled.", server.BackupCronJobName())
	}

	updated := server.DeepCopy()
	updated.Status.ObservedGeneration = server.Generation
	updated.Status.Phase = fusekiWorkloadPhase(statefulSet.Status.ReadyReplicas, server.DesiredReplicas())
	updated.Status.ConfigMapName = server.ConfigMapName()
	updated.Status.ServiceName = server.ServiceName()
	updated.Status.HeadlessServiceName = server.HeadlessServiceName()
	updated.Status.StatefulSetName = server.StatefulSetName()
	if server.Spec.BackupPolicyRef != nil {
		updated.Status.BackupCronJobName = server.BackupCronJobName()
	}
	if activeRestore != nil {
		updated.Status.ActiveRestoreName = activeRestore.Name
	}
	updated.Status.ReadyReplicas = statefulSet.Status.ReadyReplicas
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "ResourcesReconciled",
		Message:            "RDF Delta services and StatefulSet are reconciled.",
		ObservedGeneration: server.Generation,
	})
	workloadConditionStatusValue, workloadConditionReasonValue, workloadConditionMessageValue := restoreStatefulSetWorkloadCondition(activeRestore, statefulSet.Status.ReadyReplicas, server.DesiredReplicas())
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               workloadReadyConditionType,
		Status:             workloadConditionStatusValue,
		Reason:             workloadConditionReasonValue,
		Message:            workloadConditionMessageValue,
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               backupReadyConditionType,
		Status:             backupConditionStatus,
		Reason:             backupConditionReason,
		Message:            backupConditionMessage,
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               restoreReadyConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "RestoreNotRequested",
		Message:            "No active restore request is blocking RDF Delta.",
		ObservedGeneration: server.Generation,
	})
	if activeRestore != nil {
		apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
			Type:               restoreReadyConditionType,
			Status:             metav1.ConditionFalse,
			Reason:             "RestoreInProgress",
			Message:            fmt.Sprintf("RestoreRequest %q is in progress.", activeRestore.Name),
			ObservedGeneration: server.Generation,
		})
		updated.Status.Phase = "Restoring"
	}

	if !reflect.DeepEqual(server.Status, updated.Status) {
		server.Status = updated.Status
		if err := r.Status().Update(ctx, &server); err != nil {
			return ctrl.Result{}, err
		}
	}

	if server.Spec.BackupPolicyRef != nil && backupStatus.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: backupPolicyRequeueInterval}, nil
	}
	if activeRestore != nil {
		return ctrl.Result{RequeueAfter: backupPolicyRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *RDFDeltaServerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.RDFDeltaServer{}).
		Watches(&fusekiv1alpha1.BackupPolicy{}, handler.EnqueueRequestsFromMapFunc(r.requestsForBackupPolicy)).
		Watches(&fusekiv1alpha1.RestoreRequest{}, handler.EnqueueRequestsFromMapFunc(r.requestsForRestoreRequest)).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.CronJob{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func (r *RDFDeltaServerReconciler) reconcileConfigMap(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: server.ConfigMapName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "config"})
		configMap.Data = renderRDFDeltaConfigData(server)
		for key, value := range map[string]string{
			"servicePort":   strconv.FormatInt(int64(server.DesiredServicePort()), 10),
			"retentionDays": strconv.FormatInt(int64(server.DesiredRetentionDays()), 10),
			"storagePath":   fusekiv1alpha1.DefaultRDFDeltaDataMountPath,
			"serviceName":   server.ServiceName(),
		} {
			configMap.Data[key] = value
		}
		if server.Spec.BackupPolicyRef != nil {
			configMap.Data["backupPolicyRef"] = server.Spec.BackupPolicyRef.Name
		}
		if server.Spec.TLSSecretRef != nil {
			configMap.Data["tlsSecretRef"] = server.Spec.TLSSecretRef.Name
		}
		return controllerutil.SetControllerReference(server, configMap, r.Scheme)
	})
	return err
}

func (r *RDFDeltaServerReconciler) reconcileHeadlessService(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: server.HeadlessServiceName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/service-role": "headless"})
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = rdfDeltaSelectorLabels(server)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "delta",
			Port:       server.DesiredServicePort(),
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(server.DesiredServicePort()),
		}}
		return controllerutil.SetControllerReference(server, svc, r.Scheme)
	})
	return err
}

func (r *RDFDeltaServerReconciler) reconcileService(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: server.ServiceName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/service-role": "client"})
		svc.Spec.Selector = rdfDeltaSelectorLabels(server)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "delta",
			Port:       server.DesiredServicePort(),
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(server.DesiredServicePort()),
		}}
		return controllerutil.SetControllerReference(server, svc, r.Scheme)
	})
	return err
}

func (r *RDFDeltaServerReconciler) reconcileStatefulSet(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer, restoreInProgress bool) (*appsv1.StatefulSet, error) {
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: server.StatefulSetName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		statefulSet.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		statefulSet.Spec.ServiceName = server.HeadlessServiceName()
		statefulSet.Spec.Replicas = ptrTo(server.RestoreStatefulSetReplicas(restoreInProgress))
		statefulSet.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
		statefulSet.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
		statefulSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: rdfDeltaSelectorLabels(server)}
		statefulSet.Spec.Template.ObjectMeta.Labels = mergeStringMaps(rdfDeltaSelectorLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrTo(int64(30))
		statefulSet.Spec.Template.Spec.Containers = []corev1.Container{rdfDeltaContainer(server)}
		statefulSet.Spec.Template.Spec.Volumes = []corev1.Volume{{
			Name:         "operator-config",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: server.ConfigMapName()}}},
		}}
		statefulSet.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{rdfDeltaPersistentVolumeClaim(server)}
		return controllerutil.SetControllerReference(server, statefulSet, r.Scheme)
	})
	return statefulSet, err
}

func rdfDeltaLabels(server *fusekiv1alpha1.RDFDeltaServer) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "rdf-delta",
		"app.kubernetes.io/instance":   server.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/rdf-delta":  server.Name,
	}
}

func rdfDeltaSelectorLabels(server *fusekiv1alpha1.RDFDeltaServer) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "rdf-delta",
		"fuseki.apache.org/rdf-delta": server.Name,
	}
}

func rdfDeltaContainer(server *fusekiv1alpha1.RDFDeltaServer) corev1.Container {
	return corev1.Container{
		Name:            "rdf-delta",
		Image:           server.Spec.Image,
		ImagePullPolicy: server.DesiredImagePullPolicy(),
		Ports: []corev1.ContainerPort{{
			Name:          "delta",
			ContainerPort: server.DesiredServicePort(),
			Protocol:      corev1.ProtocolTCP,
		}},
		Env: []corev1.EnvVar{
			{Name: "RDF_DELTA_PORT", Value: strconv.FormatInt(int64(server.DesiredServicePort()), 10)},
			{Name: "RDF_DELTA_RETENTION_DAYS", Value: strconv.FormatInt(int64(server.DesiredRetentionDays()), 10)},
			{Name: "RDF_DELTA_SERVICE_NAME", Value: server.ServiceName()},
			{Name: "RDF_DELTA_STORAGE_PATH", Value: fusekiv1alpha1.DefaultRDFDeltaDataMountPath},
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		},
		Resources: server.Spec.Resources,
		Command:   []string{"/bin/sh", "/etc/rdf-delta/operator-config/run-rdf-delta.sh"},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      rdfDeltaDataVolumeName,
				MountPath: fusekiv1alpha1.DefaultRDFDeltaDataMountPath,
			},
			{
				Name:      "operator-config",
				MountPath: "/etc/rdf-delta/operator-config",
				ReadOnly:  true,
			},
		},
		StartupProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(server.DesiredServicePort())}},
			FailureThreshold:    30,
			PeriodSeconds:       5,
			InitialDelaySeconds: 5,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(server.DesiredServicePort())}},
			PeriodSeconds:       5,
			InitialDelaySeconds: 10,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(server.DesiredServicePort())}},
			PeriodSeconds:       10,
			InitialDelaySeconds: 15,
		},
	}
}

func (r *RDFDeltaServerReconciler) reconcileBackupCronJob(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer, policy *fusekiv1alpha1.BackupPolicy) error {
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: server.BackupCronJobName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		cronJob.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "backup"})
		cronJob.Spec.Schedule = policy.Spec.Schedule
		cronJob.Spec.Suspend = ptrTo(policy.Spec.Suspend)
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		cronJob.Spec.SuccessfulJobsHistoryLimit = ptrTo(policy.DesiredSuccessfulJobsHistoryLimit())
		cronJob.Spec.FailedJobsHistoryLimit = ptrTo(policy.DesiredFailedJobsHistoryLimit())
		cronJob.Spec.JobTemplate.ObjectMeta.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "backup-job"})
		cronJob.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "backup-job"})
		cronJob.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers = []corev1.Container{rdfDeltaBackupContainer(server, policy)}
		cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes = []corev1.Volume{{
			Name: rdfDeltaDataVolumeName,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: rdfDeltaBackupPersistentVolumeClaimName(server),
				ReadOnly:  true,
			}},
		}}
		return controllerutil.SetControllerReference(server, cronJob, r.Scheme)
	})
	return err
}

func deleteRDFDeltaBackupCronJob(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}

	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.IgnoreNotFound(c.Delete(ctx, cronJob))
}

func rdfDeltaBackupPersistentVolumeClaimName(server *fusekiv1alpha1.RDFDeltaServer) string {
	return fmt.Sprintf("%s-%s-0", rdfDeltaDataVolumeName, server.StatefulSetName())
}

func rdfDeltaBackupContainer(server *fusekiv1alpha1.RDFDeltaServer, policy *fusekiv1alpha1.BackupPolicy) corev1.Container {
	return corev1.Container{
		Name:            "backup",
		Image:           policy.DesiredBackupImage(),
		ImagePullPolicy: policy.DesiredImagePullPolicy(),
		Command:         []string{"/bin/sh", "-ec", rdfDeltaBackupScript()},
		Env: []corev1.EnvVar{
			{Name: "RDF_DELTA_SERVER_NAME", Value: server.Name},
			{Name: "RDF_DELTA_SERVER_NAMESPACE", Value: server.Namespace},
			{Name: "RDF_DELTA_STORAGE_PATH", Value: fusekiv1alpha1.DefaultRDFDeltaDataMountPath},
			{Name: "RDF_DELTA_BACKUP_PREFIX", Value: rdfDeltaBackupObjectPrefix(server, policy)},
			{Name: "S3_ENDPOINT", Value: policy.Spec.S3.Endpoint},
			{Name: "S3_BUCKET", Value: policy.Spec.S3.Bucket},
			{Name: "S3_REGION", Value: policy.Spec.S3.Region},
			{Name: "S3_INSECURE", Value: strconv.FormatBool(policy.Spec.S3.Insecure)},
			{Name: "RETENTION_MAX_BACKUPS", Value: strconv.FormatInt(int64(policy.DesiredMaxBackups()), 10)},
			{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: policy.Spec.S3.CredentialsSecretRef, Key: backupPolicyAccessKeyKey}}},
			{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: policy.Spec.S3.CredentialsSecretRef, Key: backupPolicySecretKeyKey}}},
		},
		Resources: policy.Spec.Job.Resources,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      rdfDeltaDataVolumeName,
			MountPath: fusekiv1alpha1.DefaultRDFDeltaDataMountPath,
			ReadOnly:  true,
		}},
	}
}

func rdfDeltaBackupObjectPrefix(server *fusekiv1alpha1.RDFDeltaServer, policy *fusekiv1alpha1.BackupPolicy) string {
	parts := make([]string, 0, 3)
	if trimmed := strings.Trim(policy.Spec.S3.Prefix, "/"); trimmed != "" {
		parts = append(parts, trimmed)
	}
	parts = append(parts, server.Namespace, server.Name)
	return strings.Join(parts, "/")
}

func rdfDeltaBackupScript() string {
	return `
set -eu

mc_flags=""
if [ "${S3_INSECURE:-false}" = "true" ]; then
  mc_flags="--insecure"
fi

mc ${mc_flags} alias set backup "${S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" --api S3v4 >/dev/null

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
object_dir="${S3_BUCKET}/${RDF_DELTA_BACKUP_PREFIX}"
snapshot_name="${timestamp}-${RDF_DELTA_SERVER_NAME}"

mc ${mc_flags} mirror --overwrite "${RDF_DELTA_STORAGE_PATH}/" "backup/${object_dir}/${snapshot_name}/"

listing="$(mc ${mc_flags} ls --json "backup/${object_dir}/" || true)"
objects=""
count=0
while IFS= read -r line; do
	case "${line}" in
		*'"key":"'*)
			object="${line#*\"key\":\"}"
			object="${object%%\"*}"
			object="${object%/}"
			[ -n "${object}" ] || continue
			objects="${objects}${object}
"
			count=$((count + 1))
			;;
	esac
done <<EOF
${listing}
EOF

if [ "${RETENTION_MAX_BACKUPS}" -gt 0 ] && [ "${count}" -gt "${RETENTION_MAX_BACKUPS}" ]; then
  delete_count=$((count - RETENTION_MAX_BACKUPS))
	old_ifs="${IFS}"
	IFS='
'
	index=0
	for object in ${objects}; do
    [ -n "${object}" ] || continue
		index=$((index + 1))
		if [ "${index}" -le "${delete_count}" ]; then
	  mc ${mc_flags} rm --recursive --force "backup/${object_dir}/${object}"
		fi
	done
	IFS="${old_ifs}"
fi
`
}

func (r *RDFDeltaServerReconciler) requestsForBackupPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	var servers fusekiv1alpha1.RDFDeltaServerList
	if err := r.List(ctx, &servers, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range servers.Items {
		server := &servers.Items[i]
		if server.Spec.BackupPolicyRef == nil || server.Spec.BackupPolicyRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	}

	return requests
}

func (r *RDFDeltaServerReconciler) requestsForRestoreRequest(ctx context.Context, obj client.Object) []reconcile.Request {
	request, ok := obj.(*fusekiv1alpha1.RestoreRequest)
	if !ok || request.Spec.TargetRef.Kind != fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: request.Namespace, Name: request.Spec.TargetRef.Name}}}
}

func rdfDeltaPersistentVolumeClaim(server *fusekiv1alpha1.RDFDeltaServer) corev1.PersistentVolumeClaim {
	claim := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: rdfDeltaDataVolumeName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{server.DesiredStorageAccessMode()},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: server.DesiredStorageSize()}},
		},
	}
	if server.Spec.Storage.ClassName != nil {
		claim.Spec.StorageClassName = server.Spec.Storage.ClassName
	}
	return claim
}

func rdfDeltaWorkloadSummary(server *fusekiv1alpha1.RDFDeltaServer) string {
	return fmt.Sprintf("RDF Delta service %s on port %d", server.ServiceName(), server.DesiredServicePort())
}
