package controller

import (
	"context"
	"fmt"
	"reflect"

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

type FusekiServerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiservers,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

func (r *FusekiServerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server fusekiv1alpha1.FusekiServer
	if err := r.Get(ctx, req.NamespacedName, &server); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileConfigMap(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}
	securityStatus, err := resolveFusekiWorkloadSecurityDependency(ctx, r.Client, server.Namespace, server.Spec.SecurityProfileRef, server.Spec.DatasetRefs)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, &server, securityStatus.Profile); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePVC(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}
	observabilityStatus, err := r.reconcileObservability(ctx, &server)
	if err != nil {
		return ctrl.Result{}, err
	}
	deployment, err := r.reconcileDeployment(ctx, &server, securityStatus.Profile, securityStatus.AdminSecretRef, workloadSecurityReady(securityStatus))
	if err != nil {
		return ctrl.Result{}, err
	}
	if server.Spec.SecurityProfileRef == nil || workloadSecurityReady(securityStatus) {
		if err := r.reconcileDatasetBootstrapJobs(ctx, &server); err != nil {
			return ctrl.Result{}, err
		}
	}

	updated := server.DeepCopy()
	updated.Status.ObservedGeneration = server.Generation
	updated.Status.Phase = fusekiWorkloadPhase(deployment.Status.ReadyReplicas, 1)
	updated.Status.ConfigMapName = server.ConfigMapName()
	updated.Status.ServiceName = server.ServiceName()
	updated.Status.DeploymentName = server.DeploymentName()
	updated.Status.PVCName = server.PersistentVolumeClaimName()
	updated.Status.MetricsServiceName = observabilityStatus.MetricsServiceName
	updated.Status.ReadyReplicas = deployment.Status.ReadyReplicas
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "ResourcesReconciled",
		Message:            "FusekiServer config, service, PVC, and Deployment are reconciled.",
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               workloadReadyConditionType,
		Status:             workloadConditionStatus(deployment.Status.ReadyReplicas, 1),
		Reason:             workloadConditionReason(deployment.Status.ReadyReplicas, 1),
		Message:            workloadConditionMessage("FusekiServer", deployment.Status.ReadyReplicas, 1),
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               securityReadyConditionType,
		Status:             securityStatus.Status,
		Reason:             securityStatus.Reason,
		Message:            securityStatus.Message,
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               monitoringReadyConditionType,
		Status:             observabilityStatus.ConditionStatus,
		Reason:             observabilityStatus.Reason,
		Message:            observabilityStatus.Message,
		ObservedGeneration: server.Generation,
	})

	if !reflect.DeepEqual(server.Status, updated.Status) {
		server.Status = updated.Status
		if err := r.Status().Update(ctx, &server); err != nil {
			return ctrl.Result{}, err
		}
	}

	if server.Spec.SecurityProfileRef != nil && securityStatus.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}
	if observabilityStatus.ConditionStatus != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: observabilityRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *FusekiServerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.FusekiServer{}).
		Watches(&fusekiv1alpha1.SecurityProfile{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecurityProfile)).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&batchv1.Job{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}

func (r *FusekiServerReconciler) reconcileDatasetBootstrapJobs(ctx context.Context, server *fusekiv1alpha1.FusekiServer) error {
	target := datasetBootstrapTarget{
		Kind:               "server",
		Name:               server.Name,
		Image:              server.Spec.Image,
		ImagePullPolicy:    server.DesiredImagePullPolicy(),
		WriteURL:           fmt.Sprintf("http://%s:%d", server.ServiceName(), server.DesiredHTTPPort()),
		SecurityProfileRef: server.Spec.SecurityProfileRef,
	}
	return reconcileDatasetBootstrapJobs(ctx, r.Client, r.Scheme, server, target, server.Spec.DatasetRefs, fusekiServerLabels(server))
}

func (r *FusekiServerReconciler) reconcileConfigMap(ctx context.Context, server *fusekiv1alpha1.FusekiServer) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: server.ConfigMapName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/component": "config"})
		configMap.Data = renderFusekiConfigData(server.Name, server.Spec.DatasetRefs, server.DesiredHTTPPort(), server.ServiceName())
		configMap.Data["mode"] = "single-server"
		return controllerutil.SetControllerReference(server, configMap, r.Scheme)
	})
	return err
}

func (r *FusekiServerReconciler) reconcileService(ctx context.Context, server *fusekiv1alpha1.FusekiServer, securityProfile *fusekiv1alpha1.SecurityProfile) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: server.ServiceName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/service-role": "server"})
		svc.Annotations = mergeStringMaps(nil, server.Spec.Service.Annotations)
		svc.Spec.Type = server.DesiredServiceType()
		svc.Spec.Selector = fusekiServerSelectorLabels(server)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       fusekiServicePortName(securityProfile),
			Port:       server.DesiredHTTPPort(),
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(server.DesiredHTTPPort()),
		}}
		return controllerutil.SetControllerReference(server, svc, r.Scheme)
	})
	return err
}

func (r *FusekiServerReconciler) reconcilePVC(ctx context.Context, server *fusekiv1alpha1.FusekiServer) error {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: server.PersistentVolumeClaimName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		pvc.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/component": "storage"})
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{server.DesiredStorageAccessMode()}
		pvc.Spec.Resources = corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: server.DesiredStorageSize()}}
		if server.Spec.Storage.ClassName != nil {
			pvc.Spec.StorageClassName = server.Spec.Storage.ClassName
		}
		return controllerutil.SetControllerReference(server, pvc, r.Scheme)
	})
	return err
}

func (r *FusekiServerReconciler) reconcileDeployment(ctx context.Context, server *fusekiv1alpha1.FusekiServer, securityProfile *fusekiv1alpha1.SecurityProfile, adminSecretRef *corev1.LocalObjectReference, securityReady bool) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: server.DeploymentName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		deployment.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		replicas := int32(1)
		if !securityReady {
			replicas = 0
		}
		deployment.Spec.Replicas = ptrTo(replicas)
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: fusekiServerSelectorLabels(server)}
		deployment.Spec.Template.ObjectMeta.Labels = mergeStringMaps(fusekiServerSelectorLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		deployment.Spec.Template.ObjectMeta.Annotations = nil
		if server.Spec.Observability.Logging != nil {
			deployment.Spec.Template.ObjectMeta.Annotations = mergeStringMaps(nil, server.Spec.Observability.Logging.PodAnnotations)
		}
		deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrTo(int64(30))
		containers := []corev1.Container{fusekiServerContainer(server, securityProfile, adminSecretRef)}
		if opaSidecar := fusekiOPASidecarContainer(securityProfile); opaSidecar != nil {
			containers = append(containers, *opaSidecar)
		}
		deployment.Spec.Template.Spec.Containers = containers
		volumes := []corev1.Volume{
			{
				Name:         fusekiConfigVolumeName,
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: server.ConfigMapName()}}},
			},
			fusekiDatasetConfigVolumeForRefs(server.Spec.DatasetRefs),
			{
				Name:         fusekiDataVolumeName,
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: server.PersistentVolumeClaimName()}},
			},
		}
		if securityVolume := fusekiSecurityConfigVolume(securityProfile); securityVolume != nil {
			volumes = append(volumes, *securityVolume)
		}
		if tlsVolume := fusekiSecurityTLSVolume(securityProfile); tlsVolume != nil {
			volumes = append(volumes, *tlsVolume)
		}
		deployment.Spec.Template.Spec.Volumes = volumes
		return controllerutil.SetControllerReference(server, deployment, r.Scheme)
	})
	return deployment, err
}

func (r *FusekiServerReconciler) reconcileObservability(ctx context.Context, server *fusekiv1alpha1.FusekiServer) (workloadObservabilityStatus, error) {
	var metricsAnnotations map[string]string
	var serviceMonitorLabels map[string]string
	if server.Spec.Observability.Metrics != nil {
		metricsAnnotations = server.Spec.Observability.Metrics.Service.Annotations
		if server.Spec.Observability.Metrics.ServiceMonitor != nil {
			serviceMonitorLabels = server.Spec.Observability.Metrics.ServiceMonitor.Labels
		}
	}

	return reconcileWorkloadObservability(ctx, r.Client, workloadObservabilityConfig{
		Owner:                     server,
		Scheme:                    r.Scheme,
		Labels:                    fusekiServerLabels(server),
		Selector:                  fusekiServerSelectorLabels(server),
		MetricsEnabled:            server.ObservabilityMetricsEnabled(),
		MetricsServiceName:        server.MetricsServiceName(),
		MetricsServicePort:        server.DesiredHTTPPort(),
		MetricsServiceAnnotations: metricsAnnotations,
		MetricsPath:               server.DesiredMetricsPath(),
		ServiceMonitorEnabled:     server.ObservabilityServiceMonitorEnabled(),
		ServiceMonitorInterval:    server.DesiredMetricsInterval(),
		ServiceMonitorLabels:      serviceMonitorLabels,
	})
}

func (r *FusekiServerReconciler) requestsForSecurityProfile(ctx context.Context, obj client.Object) []reconcile.Request {
	var servers fusekiv1alpha1.FusekiServerList
	if err := r.List(ctx, &servers, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range servers.Items {
		server := &servers.Items[i]
		if server.Spec.SecurityProfileRef == nil || server.Spec.SecurityProfileRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	}

	return requests
}

func fusekiServerLabels(server *fusekiv1alpha1.FusekiServer) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "fuseki-server",
		"app.kubernetes.io/instance":   server.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/server":     server.Name,
	}
}

func fusekiServerSelectorLabels(server *fusekiv1alpha1.FusekiServer) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":   "fuseki-server",
		"fuseki.apache.org/server": server.Name,
	}
}

func fusekiServerContainer(server *fusekiv1alpha1.FusekiServer, securityProfile *fusekiv1alpha1.SecurityProfile, adminSecretRef *corev1.LocalObjectReference) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "FUSEKI_BASE", Value: fusekiv1alpha1.DefaultFusekiDataMountPath},
		{Name: "FUSEKI_PORT", Value: fmt.Sprintf("%d", server.DesiredHTTPPort())},
		{Name: "FUSEKI_OPERATOR_CONFIG", Value: "/fuseki-extra/operator-config"},
	}
	env = append(env, fusekiSecurityEnvVars(securityProfile)...)
	env = append(env, fusekiAdminEnvVars(adminSecretRef)...)
	volumeMounts := []corev1.VolumeMount{
		{Name: fusekiDataVolumeName, MountPath: fusekiv1alpha1.DefaultFusekiDataMountPath},
		{Name: fusekiConfigVolumeName, MountPath: "/fuseki-extra/operator-config", ReadOnly: true},
		{Name: datasetConfigVolumeName, MountPath: "/fuseki-extra/dataset-config", ReadOnly: true},
	}
	if securityMount := fusekiSecurityConfigVolumeMount(securityProfile); securityMount != nil {
		volumeMounts = append(volumeMounts, *securityMount)
	}
	if tlsMount := fusekiSecurityTLSVolumeMount(securityProfile); tlsMount != nil {
		volumeMounts = append(volumeMounts, *tlsMount)
	}

	return corev1.Container{
		Name:            "fuseki",
		Image:           server.Spec.Image,
		ImagePullPolicy: server.DesiredImagePullPolicy(),
		Args:            []string{"/bin/sh", "/fuseki-extra/operator-config/run-fuseki.sh"},
		Ports: []corev1.ContainerPort{{
			Name:          fusekiServicePortName(securityProfile),
			ContainerPort: server.DesiredHTTPPort(),
			Protocol:      corev1.ProtocolTCP,
		}},
		Env:            env,
		Resources:      server.Spec.Resources,
		VolumeMounts:   volumeMounts,
		StartupProbe:   fusekiStartupProbe(securityProfile, server.DesiredHTTPPort()),
		ReadinessProbe: fusekiReadinessProbe(securityProfile, server.DesiredHTTPPort()),
		LivenessProbe:  fusekiLivenessProbe(securityProfile, server.DesiredHTTPPort()),
	}
}
