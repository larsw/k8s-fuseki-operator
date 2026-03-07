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

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type FusekiServerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiservers,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch

func (r *FusekiServerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server fusekiv1alpha1.FusekiServer
	if err := r.Get(ctx, req.NamespacedName, &server); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileConfigMap(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePVC(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}
	deployment, err := r.reconcileDeployment(ctx, &server)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileDatasetBootstrapJobs(ctx, &server); err != nil {
		return ctrl.Result{}, err
	}

	updated := server.DeepCopy()
	updated.Status.ObservedGeneration = server.Generation
	updated.Status.Phase = fusekiWorkloadPhase(deployment.Status.ReadyReplicas, 1)
	updated.Status.ConfigMapName = server.ConfigMapName()
	updated.Status.ServiceName = server.ServiceName()
	updated.Status.DeploymentName = server.DeploymentName()
	updated.Status.PVCName = server.PersistentVolumeClaimName()
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

	if !reflect.DeepEqual(server.Status, updated.Status) {
		server.Status = updated.Status
		if err := r.Status().Update(ctx, &server); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *FusekiServerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.FusekiServer{}).
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

func (r *FusekiServerReconciler) reconcileService(ctx context.Context, server *fusekiv1alpha1.FusekiServer) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: server.ServiceName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/service-role": "server"})
		svc.Annotations = mergeStringMaps(nil, server.Spec.Service.Annotations)
		svc.Spec.Type = server.DesiredServiceType()
		svc.Spec.Selector = fusekiServerSelectorLabels(server)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
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

func (r *FusekiServerReconciler) reconcileDeployment(ctx context.Context, server *fusekiv1alpha1.FusekiServer) (*appsv1.Deployment, error) {
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: server.DeploymentName(), Namespace: server.Namespace}}
	_, adminSecretRef, err := resolveDatasetSecurity(ctx, r.Client, server.Namespace, server.Spec.SecurityProfileRef)
	if err != nil {
		return nil, err
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		deployment.Labels = mergeStringMaps(fusekiServerLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: fusekiServerSelectorLabels(server)}
		deployment.Spec.Template.ObjectMeta.Labels = mergeStringMaps(fusekiServerSelectorLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrTo(int64(30))
		deployment.Spec.Template.Spec.Containers = []corev1.Container{fusekiServerContainer(server, adminSecretRef)}
		deployment.Spec.Template.Spec.Volumes = []corev1.Volume{
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
		return controllerutil.SetControllerReference(server, deployment, r.Scheme)
	})
	return deployment, err
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

func fusekiServerContainer(server *fusekiv1alpha1.FusekiServer, adminSecretRef *corev1.LocalObjectReference) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "FUSEKI_BASE", Value: fusekiv1alpha1.DefaultFusekiDataMountPath},
		{Name: "FUSEKI_PORT", Value: fmt.Sprintf("%d", server.DesiredHTTPPort())},
		{Name: "FUSEKI_OPERATOR_CONFIG", Value: "/fuseki-extra/operator-config"},
	}
	env = append(env, fusekiAdminEnvVars(adminSecretRef)...)

	return corev1.Container{
		Name:            "fuseki",
		Image:           server.Spec.Image,
		ImagePullPolicy: server.DesiredImagePullPolicy(),
		Args:            []string{"/bin/sh", "/fuseki-extra/operator-config/run-fuseki.sh"},
		Ports: []corev1.ContainerPort{{
			Name:          "http",
			ContainerPort: server.DesiredHTTPPort(),
			Protocol:      corev1.ProtocolTCP,
		}},
		Env:       env,
		Resources: server.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{Name: fusekiDataVolumeName, MountPath: fusekiv1alpha1.DefaultFusekiDataMountPath},
			{Name: fusekiConfigVolumeName, MountPath: "/fuseki-extra/operator-config", ReadOnly: true},
			{Name: datasetConfigVolumeName, MountPath: "/fuseki-extra/dataset-config", ReadOnly: true},
		},
		StartupProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(server.DesiredHTTPPort())}}, FailureThreshold: 30, PeriodSeconds: 5, InitialDelaySeconds: 5},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(server.DesiredHTTPPort())}}, PeriodSeconds: 5, InitialDelaySeconds: 10},
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/$/ping", Port: intstr.FromInt32(server.DesiredHTTPPort())}}, PeriodSeconds: 10, InitialDelaySeconds: 15},
	}
}
