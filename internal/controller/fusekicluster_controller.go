package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
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

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

const (
	configuredConditionType    = "Configured"
	workloadReadyConditionType = "WorkloadReady"
	fusekiDataVolumeName       = "data"
	fusekiConfigVolumeName     = "operator-config"
	datasetConfigVolumeName    = "dataset-config"
)

type FusekiClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiclusters,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;services;pods;persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch

func (r *FusekiClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster fusekiv1alpha1.FusekiCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileConfigMap(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileHeadlessService(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, &cluster, cluster.ReadServiceName(), "read", cluster.Spec.Services.ReadAnnotations); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, &cluster, cluster.WriteServiceName(), "write", cluster.Spec.Services.WriteAnnotations); err != nil {
		return ctrl.Result{}, err
	}

	securityStatus, err := resolveSecurityDependency(ctx, r.Client, cluster.Namespace, cluster.Spec.SecurityProfileRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	statefulSet, err := r.reconcileStatefulSet(ctx, &cluster, securityStatus.AdminSecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	writePodName, err := r.reconcileWriteLease(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcilePodRouting(ctx, &cluster, writePodName); err != nil {
		return ctrl.Result{}, err
	}
	if cluster.Spec.SecurityProfileRef == nil || securityStatus.Status == metav1.ConditionTrue {
		if err := r.reconcileDatasetBootstrapJobs(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	updated := cluster.DeepCopy()
	updated.Status.ObservedGeneration = cluster.Generation
	updated.Status.Phase = fusekiWorkloadPhase(statefulSet.Status.ReadyReplicas, cluster.DesiredReplicas())
	updated.Status.ConfigMapName = cluster.ConfigMapName()
	updated.Status.HeadlessServiceName = cluster.HeadlessServiceName()
	updated.Status.ReadServiceName = cluster.ReadServiceName()
	updated.Status.WriteServiceName = cluster.WriteServiceName()
	updated.Status.WriteLeaseName = cluster.WriteLeaseName()
	updated.Status.ActiveWritePod = writePodName
	updated.Status.StatefulSetName = cluster.StatefulSetName()
	updated.Status.ReadyReplicas = statefulSet.Status.ReadyReplicas
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "ResourcesReconciled",
		Message:            "Fuseki config, services, and StatefulSet are reconciled.",
		ObservedGeneration: cluster.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               workloadReadyConditionType,
		Status:             workloadConditionStatus(statefulSet.Status.ReadyReplicas, cluster.DesiredReplicas()),
		Reason:             workloadConditionReason(statefulSet.Status.ReadyReplicas, cluster.DesiredReplicas()),
		Message:            workloadConditionMessage("Fuseki", statefulSet.Status.ReadyReplicas, cluster.DesiredReplicas()),
		ObservedGeneration: cluster.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               securityReadyConditionType,
		Status:             securityStatus.Status,
		Reason:             securityStatus.Reason,
		Message:            securityStatus.Message,
		ObservedGeneration: cluster.Generation,
	})

	if !reflect.DeepEqual(cluster.Status, updated.Status) {
		cluster.Status = updated.Status
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	if cluster.Spec.SecurityProfileRef != nil && securityStatus.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *FusekiClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.FusekiCluster{}).
		Watches(&fusekiv1alpha1.SecurityProfile{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecurityProfile)).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.Job{}).
		Owns(&coordinationv1.Lease{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func (r *FusekiClusterReconciler) reconcileDatasetBootstrapJobs(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster) error {
	target := datasetBootstrapTarget{
		Kind:               "cluster",
		Name:               cluster.Name,
		Image:              cluster.Spec.Image,
		ImagePullPolicy:    cluster.DesiredImagePullPolicy(),
		WriteURL:           fmt.Sprintf("http://%s:%d", cluster.WriteServiceName(), cluster.DesiredHTTPPort()),
		SecurityProfileRef: cluster.Spec.SecurityProfileRef,
	}
	return reconcileDatasetBootstrapJobs(ctx, r.Client, r.Scheme, cluster, target, cluster.Spec.DatasetRefs, clusterLabels(cluster))
}

func (r *FusekiClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.ConfigMapName(),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = mergeStringMaps(clusterLabels(cluster), map[string]string{"fuseki.apache.org/component": "config"})
		configMap.Data = renderFusekiConfigData(cluster.Name, cluster.Spec.DatasetRefs, cluster.DesiredHTTPPort(), cluster.WriteServiceName())
		for key, value := range map[string]string{
			"image":             cluster.Spec.Image,
			"replicas":          fmt.Sprintf("%d", cluster.DesiredReplicas()),
			"httpPort":          fmt.Sprintf("%d", cluster.DesiredHTTPPort()),
			"rdfDeltaServerRef": cluster.Spec.RDFDeltaServerRef.Name,
			"datasetRefs":       joinLocalObjectReferences(cluster.Spec.DatasetRefs),
			"headlessService":   cluster.HeadlessServiceName(),
			"writeLease":        cluster.WriteLeaseName(),
			"statefulSet":       cluster.StatefulSetName(),
			"readService":       cluster.ReadServiceName(),
			"writeService":      cluster.WriteServiceName(),
		} {
			configMap.Data[key] = value
		}

		return controllerutil.SetControllerReference(cluster, configMap, r.Scheme)
	})

	return err
}

func (r *FusekiClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.HeadlessServiceName(),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(clusterLabels(cluster), map[string]string{"fuseki.apache.org/service-role": "headless"})
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = clusterSelectorLabels(cluster)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       cluster.DesiredHTTPPort(),
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(cluster.DesiredHTTPPort()),
		}}

		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})

	return err
}

func (r *FusekiClusterReconciler) reconcileService(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster, name, role string, annotations map[string]string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(clusterLabels(cluster), map[string]string{"fuseki.apache.org/service-role": role})
		svc.Annotations = mergeStringMaps(nil, annotations)
		svc.Spec.Type = cluster.DesiredServiceType()
		svc.Spec.Selector = serviceSelectorLabels(cluster, role)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       cluster.DesiredHTTPPort(),
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(cluster.DesiredHTTPPort()),
		}}

		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})

	return err
}

func (r *FusekiClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster, adminSecretRef *corev1.LocalObjectReference) (*appsv1.StatefulSet, error) {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.StatefulSetName(),
			Namespace: cluster.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		statefulSet.Labels = mergeStringMaps(clusterLabels(cluster), map[string]string{"fuseki.apache.org/component": "server"})
		statefulSet.Spec.ServiceName = cluster.HeadlessServiceName()
		statefulSet.Spec.Replicas = ptrTo(cluster.DesiredReplicas())
		statefulSet.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
		statefulSet.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
		statefulSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: clusterSelectorLabels(cluster)}
		statefulSet.Spec.Template.ObjectMeta.Labels = mergeStringMaps(clusterSelectorLabels(cluster), map[string]string{
			"fuseki.apache.org/component":  "server",
			"fuseki.apache.org/read-route": "true",
		})
		statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrTo(int64(30))
		statefulSet.Spec.Template.Spec.Containers = []corev1.Container{fusekiContainer(cluster, adminSecretRef)}
		statefulSet.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name:         fusekiConfigVolumeName,
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.ConfigMapName()}}},
			},
			fusekiDatasetConfigVolumeForRefs(cluster.Spec.DatasetRefs),
		}
		statefulSet.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{fusekiPersistentVolumeClaim(cluster)}

		return controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme)
	})

	return statefulSet, err
}

func (r *FusekiClusterReconciler) requestsForSecurityProfile(ctx context.Context, obj client.Object) []reconcile.Request {
	var clusters fusekiv1alpha1.FusekiClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.Spec.SecurityProfileRef == nil || cluster.Spec.SecurityProfileRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	}

	return requests
}

func (r *FusekiClusterReconciler) reconcileWriteLease(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster) (string, error) {
	pods, err := r.listClusterPods(ctx, cluster)
	if err != nil {
		return "", err
	}

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.WriteLeaseName(),
			Namespace: cluster.Namespace,
		},
	}

	selectedWritePod := ""
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, lease, func() error {
		lease.Labels = mergeStringMaps(clusterLabels(cluster), map[string]string{"fuseki.apache.org/component": "write-lease"})
		selectedWritePod = selectLeaseHolder(pods, derefStringPtr(lease.Spec.HolderIdentity))
		now := metav1.NewMicroTime(time.Now())
		duration := cluster.DesiredLeaseDurationSeconds()
		lease.Spec.LeaseDurationSeconds = &duration
		lease.Spec.RenewTime = &now
		if selectedWritePod == "" {
			lease.Spec.HolderIdentity = nil
			lease.Spec.AcquireTime = nil
		} else {
			if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != selectedWritePod {
				lease.Spec.AcquireTime = &now
			}
			lease.Spec.HolderIdentity = ptrTo(selectedWritePod)
		}

		return controllerutil.SetControllerReference(cluster, lease, r.Scheme)
	})

	return selectedWritePod, err
}

func (r *FusekiClusterReconciler) reconcilePodRouting(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster, writePodName string) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(clusterSelectorLabels(cluster))); err != nil {
		return err
	}

	for index := range pods.Items {
		pod := &pods.Items[index]
		updated := pod.DeepCopy()
		if updated.Labels == nil {
			updated.Labels = map[string]string{}
		}

		updated.Labels["fuseki.apache.org/read-route"] = "true"
		if pod.Name == writePodName && writePodName != "" {
			updated.Labels["fuseki.apache.org/write-route"] = "true"
			updated.Labels["fuseki.apache.org/lease-holder"] = "true"
		} else {
			delete(updated.Labels, "fuseki.apache.org/write-route")
			delete(updated.Labels, "fuseki.apache.org/lease-holder")
		}

		if reflect.DeepEqual(pod.Labels, updated.Labels) {
			continue
		}

		if err := r.Update(ctx, updated); err != nil {
			return err
		}
	}

	return nil
}

func (r *FusekiClusterReconciler) listClusterPods(ctx context.Context, cluster *fusekiv1alpha1.FusekiCluster) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(clusterSelectorLabels(cluster))); err != nil {
		return nil, err
	}

	sort.Slice(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})

	return podList.Items, nil
}

func clusterLabels(cluster *fusekiv1alpha1.FusekiCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "fuseki",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/cluster":    cluster.Name,
	}
}

func clusterSelectorLabels(cluster *fusekiv1alpha1.FusekiCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":    "fuseki",
		"fuseki.apache.org/cluster": cluster.Name,
	}
}

func serviceSelectorLabels(cluster *fusekiv1alpha1.FusekiCluster, role string) map[string]string {
	selector := mergeStringMaps(nil, clusterSelectorLabels(cluster))
	selector["fuseki.apache.org/"+role+"-route"] = "true"
	return selector
}

func fusekiContainer(cluster *fusekiv1alpha1.FusekiCluster, adminSecretRef *corev1.LocalObjectReference) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "FUSEKI_BASE", Value: fusekiv1alpha1.DefaultFusekiDataMountPath},
		{Name: "FUSEKI_CLUSTER", Value: cluster.Name},
		{Name: "FUSEKI_OPERATOR_CONFIG", Value: "/fuseki-extra/operator-config"},
		{Name: "FUSEKI_WRITE_LEASE", Value: cluster.WriteLeaseName()},
		{Name: "RDF_DELTA_SERVER", Value: cluster.Spec.RDFDeltaServerRef.Name},
		{Name: "FUSEKI_DATASETS", Value: joinLocalObjectReferences(cluster.Spec.DatasetRefs)},
		{Name: "FUSEKI_READ_SERVICE", Value: cluster.ReadServiceName()},
		{Name: "FUSEKI_WRITE_SERVICE", Value: cluster.WriteServiceName()},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
	}
	env = append(env, fusekiAdminEnvVars(adminSecretRef)...)

	return corev1.Container{
		Name:            "fuseki",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: cluster.DesiredImagePullPolicy(),
		Ports: []corev1.ContainerPort{{
			Name:          "http",
			ContainerPort: cluster.DesiredHTTPPort(),
			Protocol:      corev1.ProtocolTCP,
		}},
		Env:       env,
		Resources: cluster.Spec.Resources,
		Args:      []string{"/bin/sh", "/fuseki-extra/operator-config/run-fuseki.sh"},
		VolumeMounts: []corev1.VolumeMount{
			{Name: fusekiDataVolumeName, MountPath: fusekiv1alpha1.DefaultFusekiDataMountPath},
			{Name: fusekiConfigVolumeName, MountPath: "/fuseki-extra/operator-config", ReadOnly: true},
			{Name: datasetConfigVolumeName, MountPath: "/fuseki-extra/dataset-config", ReadOnly: true},
		},
		StartupProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(cluster.DesiredHTTPPort())}},
			FailureThreshold:    30,
			PeriodSeconds:       5,
			InitialDelaySeconds: 5,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(cluster.DesiredHTTPPort())}},
			PeriodSeconds:       5,
			InitialDelaySeconds: 10,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(cluster.DesiredHTTPPort())}},
			PeriodSeconds:       10,
			InitialDelaySeconds: 15,
		},
	}
}

func fusekiPersistentVolumeClaim(cluster *fusekiv1alpha1.FusekiCluster) corev1.PersistentVolumeClaim {
	claim := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: fusekiDataVolumeName},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{cluster.DesiredStorageAccessMode()},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: cluster.DesiredStorageSize()}},
		},
	}

	if cluster.Spec.Storage.ClassName != nil {
		claim.Spec.StorageClassName = cluster.Spec.Storage.ClassName
	}

	return claim
}

func fusekiWorkloadPhase(readyReplicas, desiredReplicas int32) string {
	if desiredReplicas > 0 && readyReplicas >= desiredReplicas {
		return "Running"
	}

	return "Provisioning"
}

func workloadConditionStatus(readyReplicas, desiredReplicas int32) metav1.ConditionStatus {
	if desiredReplicas > 0 && readyReplicas >= desiredReplicas {
		return metav1.ConditionTrue
	}

	return metav1.ConditionFalse
}

func workloadConditionReason(readyReplicas, desiredReplicas int32) string {
	if desiredReplicas > 0 && readyReplicas >= desiredReplicas {
		return "ReadyReplicasSatisfied"
	}

	return "WaitingForReplicas"
}

func workloadConditionMessage(workload string, readyReplicas, desiredReplicas int32) string {
	return fmt.Sprintf("%s workload has %d/%d ready replicas.", workload, readyReplicas, desiredReplicas)
}

func ptrTo[T any](value T) *T {
	return &value
}

func derefStringPtr(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

func selectLeaseHolder(pods []corev1.Pod, currentHolder string) string {
	for _, pod := range pods {
		if pod.Name == currentHolder && podIsReady(&pod) {
			return pod.Name
		}
	}

	for _, pod := range pods {
		if podIsReady(&pod) {
			return pod.Name
		}
	}

	return ""
}

func podIsReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func joinLocalObjectReferences(refs []corev1.LocalObjectReference) string {
	if len(refs) == 0 {
		return ""
	}

	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.Name == "" {
			continue
		}
		names = append(names, ref.Name)
	}

	return strings.Join(names, ",")
}

func mergeStringMaps(base, overrides map[string]string) map[string]string {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}

	merged := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}

	return merged
}
