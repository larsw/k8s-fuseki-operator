package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
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

const rdfDeltaDataVolumeName = "data"

type RDFDeltaServerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch

func (r *RDFDeltaServerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var server fusekiv1alpha1.RDFDeltaServer
	if err := r.Get(ctx, req.NamespacedName, &server); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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

	statefulSet, err := r.reconcileStatefulSet(ctx, &server)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := server.DeepCopy()
	updated.Status.ObservedGeneration = server.Generation
	updated.Status.Phase = fusekiWorkloadPhase(statefulSet.Status.ReadyReplicas, server.DesiredReplicas())
	updated.Status.ConfigMapName = server.ConfigMapName()
	updated.Status.ServiceName = server.ServiceName()
	updated.Status.HeadlessServiceName = server.HeadlessServiceName()
	updated.Status.StatefulSetName = server.StatefulSetName()
	updated.Status.ReadyReplicas = statefulSet.Status.ReadyReplicas
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "ResourcesReconciled",
		Message:            "RDF Delta services and StatefulSet are reconciled.",
		ObservedGeneration: server.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               workloadReadyConditionType,
		Status:             workloadConditionStatus(statefulSet.Status.ReadyReplicas, server.DesiredReplicas()),
		Reason:             workloadConditionReason(statefulSet.Status.ReadyReplicas, server.DesiredReplicas()),
		Message:            workloadConditionMessage("RDF Delta", statefulSet.Status.ReadyReplicas, server.DesiredReplicas()),
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

func (r *RDFDeltaServerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.RDFDeltaServer{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
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

func (r *RDFDeltaServerReconciler) reconcileStatefulSet(ctx context.Context, server *fusekiv1alpha1.RDFDeltaServer) (*appsv1.StatefulSet, error) {
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: server.StatefulSetName(), Namespace: server.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		statefulSet.Labels = mergeStringMaps(rdfDeltaLabels(server), map[string]string{"fuseki.apache.org/component": "server"})
		statefulSet.Spec.ServiceName = server.HeadlessServiceName()
		statefulSet.Spec.Replicas = ptrTo(server.DesiredReplicas())
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
