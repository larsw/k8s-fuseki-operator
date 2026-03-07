package controller

import (
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

type EndpointReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type endpointTargetResolution struct {
	ReadSelector  map[string]string
	WriteSelector map[string]string
	Port          int32
	Reason        string
	Message       string
	Status        metav1.ConditionStatus
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=endpoints,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=endpoints/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiclusters;fusekiservers;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch

func (r *EndpointReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var endpoint fusekiv1alpha1.Endpoint
	if err := r.Get(ctx, req.NamespacedName, &endpoint); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	target, err := r.resolveTarget(ctx, &endpoint)
	if err != nil {
		return ctrl.Result{}, err
	}
	securityStatus, err := resolveSecurityDependency(ctx, r.Client, endpoint.Namespace, endpoint.Spec.SecurityProfileRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if target.Status == metav1.ConditionTrue {
		if err := r.reconcileService(ctx, &endpoint, endpoint.ReadServiceName(), "read", endpoint.DesiredReadServiceType(), endpoint.Spec.Read.Annotations, target.ReadSelector, target.Port); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileService(ctx, &endpoint, endpoint.WriteServiceName(), "write", endpoint.DesiredWriteServiceType(), endpoint.Spec.Write.Annotations, target.WriteSelector, target.Port); err != nil {
			return ctrl.Result{}, err
		}
	}

	updated := endpoint.DeepCopy()
	updated.Status.ObservedGeneration = endpoint.Generation
	updated.Status.ReadServiceName = endpoint.ReadServiceName()
	updated.Status.WriteServiceName = endpoint.WriteServiceName()
	updated.Status.Phase = "Ready"
	if target.Status != metav1.ConditionTrue || securityStatus.Status != metav1.ConditionTrue {
		updated.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             target.Status,
		Reason:             target.Reason,
		Message:            target.Message,
		ObservedGeneration: endpoint.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               securityReadyConditionType,
		Status:             securityStatus.Status,
		Reason:             securityStatus.Reason,
		Message:            securityStatus.Message,
		ObservedGeneration: endpoint.Generation,
	})

	if !reflect.DeepEqual(endpoint.Status, updated.Status) {
		endpoint.Status = updated.Status
		if err := r.Status().Update(ctx, &endpoint); err != nil {
			return ctrl.Result{}, err
		}
	}

	if target.Status != metav1.ConditionTrue || (endpoint.Spec.SecurityProfileRef != nil && securityStatus.Status != metav1.ConditionTrue) {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *EndpointReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.Endpoint{}).
		Watches(&fusekiv1alpha1.FusekiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&fusekiv1alpha1.FusekiServer{}, handler.EnqueueRequestsFromMapFunc(r.requestsForServer)).
		Watches(&fusekiv1alpha1.SecurityProfile{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecurityProfile)).
		Owns(&corev1.Service{}).
		Complete(r)
}

func (r *EndpointReconciler) resolveTarget(ctx context.Context, endpoint *fusekiv1alpha1.Endpoint) (endpointTargetResolution, error) {
	switch endpoint.Spec.TargetRef.Kind {
	case fusekiv1alpha1.EndpointTargetKindFusekiCluster:
		var cluster fusekiv1alpha1.FusekiCluster
		if err := r.Get(ctx, client.ObjectKey{Namespace: endpoint.Namespace, Name: endpoint.Spec.TargetRef.Name}, &cluster); err != nil {
			if apierrors.IsNotFound(err) {
				return endpointTargetResolution{Status: metav1.ConditionFalse, Reason: "TargetNotFound", Message: fmt.Sprintf("Waiting for FusekiCluster %q.", endpoint.Spec.TargetRef.Name)}, nil
			}
			return endpointTargetResolution{}, err
		}
		return endpointTargetResolution{
			Status:        metav1.ConditionTrue,
			Reason:        "TargetResolved",
			Message:       fmt.Sprintf("Endpoint target FusekiCluster %q is resolved.", cluster.Name),
			ReadSelector:  serviceSelectorLabels(&cluster, "read"),
			WriteSelector: serviceSelectorLabels(&cluster, "write"),
			Port:          cluster.DesiredHTTPPort(),
		}, nil
	case fusekiv1alpha1.EndpointTargetKindFusekiServer:
		var server fusekiv1alpha1.FusekiServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: endpoint.Namespace, Name: endpoint.Spec.TargetRef.Name}, &server); err != nil {
			if apierrors.IsNotFound(err) {
				return endpointTargetResolution{Status: metav1.ConditionFalse, Reason: "TargetNotFound", Message: fmt.Sprintf("Waiting for FusekiServer %q.", endpoint.Spec.TargetRef.Name)}, nil
			}
			return endpointTargetResolution{}, err
		}
		selector := fusekiServerSelectorLabels(&server)
		return endpointTargetResolution{
			Status:        metav1.ConditionTrue,
			Reason:        "TargetResolved",
			Message:       fmt.Sprintf("Endpoint target FusekiServer %q is resolved.", server.Name),
			ReadSelector:  selector,
			WriteSelector: selector,
			Port:          server.DesiredHTTPPort(),
		}, nil
	default:
		return endpointTargetResolution{Status: metav1.ConditionFalse, Reason: "UnsupportedTargetKind", Message: fmt.Sprintf("Endpoint target kind %q is not supported.", endpoint.Spec.TargetRef.Kind)}, nil
	}
}

func (r *EndpointReconciler) reconcileService(ctx context.Context, endpoint *fusekiv1alpha1.Endpoint, name, role string, serviceType corev1.ServiceType, annotations, selector map[string]string, port int32) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: endpoint.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(endpointLabels(endpoint), map[string]string{"fuseki.apache.org/service-role": role})
		svc.Annotations = mergeStringMaps(nil, annotations)
		svc.Spec.Type = serviceType
		svc.Spec.Selector = mergeStringMaps(nil, selector)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       port,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(port),
		}}
		return controllerutil.SetControllerReference(endpoint, svc, r.Scheme)
	})
	return err
}

func endpointLabels(endpoint *fusekiv1alpha1.Endpoint) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "fuseki-endpoint",
		"app.kubernetes.io/instance":   endpoint.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/endpoint":   endpoint.Name,
	}
}

func (r *EndpointReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForTarget(ctx, obj.GetNamespace(), fusekiv1alpha1.EndpointTargetKindFusekiCluster, obj.GetName())
}

func (r *EndpointReconciler) requestsForServer(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForTarget(ctx, obj.GetNamespace(), fusekiv1alpha1.EndpointTargetKindFusekiServer, obj.GetName())
}

func (r *EndpointReconciler) requestsForTarget(ctx context.Context, namespace string, kind fusekiv1alpha1.EndpointTargetKind, name string) []reconcile.Request {
	var endpoints fusekiv1alpha1.EndpointList
	if err := r.List(ctx, &endpoints, client.InNamespace(namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range endpoints.Items {
		endpoint := &endpoints.Items[i]
		if endpoint.Spec.TargetRef.Kind != kind || endpoint.Spec.TargetRef.Name != name {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(endpoint)})
	}

	return requests
}

func (r *EndpointReconciler) requestsForSecurityProfile(ctx context.Context, obj client.Object) []reconcile.Request {
	var endpoints fusekiv1alpha1.EndpointList
	if err := r.List(ctx, &endpoints, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range endpoints.Items {
		endpoint := &endpoints.Items[i]
		if endpoint.Spec.SecurityProfileRef == nil || endpoint.Spec.SecurityProfileRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(endpoint)})
	}

	return requests
}
