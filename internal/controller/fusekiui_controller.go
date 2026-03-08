package controller

import (
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type FusekiUIReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type fusekiUITargetResolution struct {
	Selector           map[string]string
	Port               int32
	SecurityProfileRef *corev1.LocalObjectReference
	Reason             string
	Message            string
	Status             metav1.ConditionStatus
}

type fusekiUIExposureStatus struct {
	IngressStatus  metav1.ConditionStatus
	IngressReason  string
	IngressMessage string
	IngressName    string
	GatewayStatus  metav1.ConditionStatus
	GatewayReason  string
	GatewayMessage string
	HTTPRouteName  string
}

const (
	ingressReadyConditionType = "IngressReady"
	gatewayReadyConditionType = "GatewayReady"
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}
var ingressGVK = schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiuis,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiuis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=fusekiclusters;fusekiservers;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete

func (r *FusekiUIReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ui fusekiv1alpha1.FusekiUI
	if err := r.Get(ctx, req.NamespacedName, &ui); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	target, err := r.resolveTarget(ctx, &ui)
	if err != nil {
		return ctrl.Result{}, err
	}
	securityStatus, err := resolveSecurityDependency(ctx, r.Client, ui.Namespace, target.SecurityProfileRef)
	if err != nil {
		return ctrl.Result{}, err
	}

	if target.Status == metav1.ConditionTrue {
		if err := r.reconcileService(ctx, &ui, securityStatus.Profile, target.Selector, target.Port); err != nil {
			return ctrl.Result{}, err
		}
	}
	exposureStatus, err := r.reconcileExposure(ctx, &ui, target, securityStatus.Profile)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := ui.DeepCopy()
	updated.Status.ObservedGeneration = ui.Generation
	updated.Status.ServiceName = ui.ServiceName()
	updated.Status.IngressName = exposureStatus.IngressName
	updated.Status.HTTPRouteName = exposureStatus.HTTPRouteName
	updated.Status.Phase = "Ready"
	if target.Status != metav1.ConditionTrue || securityStatus.Status != metav1.ConditionTrue || exposureStatus.IngressStatus != metav1.ConditionTrue || exposureStatus.GatewayStatus != metav1.ConditionTrue {
		updated.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             target.Status,
		Reason:             target.Reason,
		Message:            target.Message,
		ObservedGeneration: ui.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               securityReadyConditionType,
		Status:             securityStatus.Status,
		Reason:             securityStatus.Reason,
		Message:            securityStatus.Message,
		ObservedGeneration: ui.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               ingressReadyConditionType,
		Status:             exposureStatus.IngressStatus,
		Reason:             exposureStatus.IngressReason,
		Message:            exposureStatus.IngressMessage,
		ObservedGeneration: ui.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               gatewayReadyConditionType,
		Status:             exposureStatus.GatewayStatus,
		Reason:             exposureStatus.GatewayReason,
		Message:            exposureStatus.GatewayMessage,
		ObservedGeneration: ui.Generation,
	})

	if !reflect.DeepEqual(ui.Status, updated.Status) {
		ui.Status = updated.Status
		if err := r.Status().Update(ctx, &ui); err != nil {
			return ctrl.Result{}, err
		}
	}

	if target.Status != metav1.ConditionTrue || (target.SecurityProfileRef != nil && securityStatus.Status != metav1.ConditionTrue) || exposureStatus.IngressStatus != metav1.ConditionTrue || exposureStatus.GatewayStatus != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *FusekiUIReconciler) reconcileExposure(ctx context.Context, ui *fusekiv1alpha1.FusekiUI, target fusekiUITargetResolution, securityProfile *fusekiv1alpha1.SecurityProfile) (fusekiUIExposureStatus, error) {
	status := fusekiUIExposureStatus{
		IngressStatus:  metav1.ConditionTrue,
		IngressReason:  "IngressNotConfigured",
		IngressMessage: "FusekiUI ingress exposure is not configured.",
		GatewayStatus:  metav1.ConditionTrue,
		GatewayReason:  "GatewayNotConfigured",
		GatewayMessage: "FusekiUI gateway exposure is not configured.",
	}

	if ui.Spec.Ingress == nil {
		if err := deleteFusekiUIIngress(ctx, r.Client, ui.Namespace, ui.IngressName()); err != nil {
			return fusekiUIExposureStatus{}, err
		}
	} else if target.Status != metav1.ConditionTrue {
		status.IngressStatus = metav1.ConditionFalse
		status.IngressReason = "TargetNotReady"
		status.IngressMessage = "Waiting for the FusekiUI target before reconciling the Ingress."
		status.IngressName = ui.IngressName()
	} else {
		if err := reconcileFusekiUIIngress(ctx, r.Client, r.Scheme, ui, target.Port); err != nil {
			return fusekiUIExposureStatus{}, err
		}
		status.IngressReason = "IngressReady"
		status.IngressMessage = fmt.Sprintf("Ingress %q is reconciled.", ui.IngressName())
		status.IngressName = ui.IngressName()
	}

	if ui.Spec.Gateway == nil {
		if err := deleteFusekiUIHTTPRoute(ctx, r.Client, ui.Namespace, ui.HTTPRouteName()); err != nil {
			return fusekiUIExposureStatus{}, err
		}
	} else if target.Status != metav1.ConditionTrue {
		status.GatewayStatus = metav1.ConditionFalse
		status.GatewayReason = "TargetNotReady"
		status.GatewayMessage = "Waiting for the FusekiUI target before reconciling the HTTPRoute."
		status.HTTPRouteName = ui.HTTPRouteName()
	} else {
		if err := reconcileFusekiUIHTTPRoute(ctx, r.Client, r.Scheme, ui, securityProfile, target.Port); err != nil {
			if apimeta.IsNoMatchError(err) {
				status.GatewayStatus = metav1.ConditionFalse
				status.GatewayReason = "GatewayAPIUnavailable"
				status.GatewayMessage = "Gateway API HTTPRoute CRD is not available in the cluster."
				status.HTTPRouteName = ui.HTTPRouteName()
				return status, nil
			}
			return fusekiUIExposureStatus{}, err
		}
		status.GatewayReason = "GatewayReady"
		status.GatewayMessage = fmt.Sprintf("HTTPRoute %q is reconciled.", ui.HTTPRouteName())
		status.HTTPRouteName = ui.HTTPRouteName()
	}

	return status, nil
}

func (r *FusekiUIReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.FusekiUI{}).
		Watches(&fusekiv1alpha1.FusekiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&fusekiv1alpha1.FusekiServer{}, handler.EnqueueRequestsFromMapFunc(r.requestsForServer)).
		Watches(&fusekiv1alpha1.SecurityProfile{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecurityProfile)).
		Owns(&corev1.Service{}).
		Complete(r)
}

func (r *FusekiUIReconciler) resolveTarget(ctx context.Context, ui *fusekiv1alpha1.FusekiUI) (fusekiUITargetResolution, error) {
	switch ui.Spec.TargetRef.Kind {
	case fusekiv1alpha1.EndpointTargetKindFusekiCluster:
		var cluster fusekiv1alpha1.FusekiCluster
		if err := r.Get(ctx, client.ObjectKey{Namespace: ui.Namespace, Name: ui.Spec.TargetRef.Name}, &cluster); err != nil {
			if apierrors.IsNotFound(err) {
				return fusekiUITargetResolution{Status: metav1.ConditionFalse, Reason: "TargetNotFound", Message: fmt.Sprintf("Waiting for FusekiCluster %q.", ui.Spec.TargetRef.Name)}, nil
			}
			return fusekiUITargetResolution{}, err
		}
		return fusekiUITargetResolution{
			Status:             metav1.ConditionTrue,
			Reason:             "TargetResolved",
			Message:            fmt.Sprintf("FusekiUI target FusekiCluster %q is resolved.", cluster.Name),
			Selector:           serviceSelectorLabels(&cluster, "write"),
			Port:               cluster.DesiredHTTPPort(),
			SecurityProfileRef: cluster.Spec.SecurityProfileRef,
		}, nil
	case fusekiv1alpha1.EndpointTargetKindFusekiServer:
		var server fusekiv1alpha1.FusekiServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: ui.Namespace, Name: ui.Spec.TargetRef.Name}, &server); err != nil {
			if apierrors.IsNotFound(err) {
				return fusekiUITargetResolution{Status: metav1.ConditionFalse, Reason: "TargetNotFound", Message: fmt.Sprintf("Waiting for FusekiServer %q.", ui.Spec.TargetRef.Name)}, nil
			}
			return fusekiUITargetResolution{}, err
		}
		return fusekiUITargetResolution{
			Status:             metav1.ConditionTrue,
			Reason:             "TargetResolved",
			Message:            fmt.Sprintf("FusekiUI target FusekiServer %q is resolved.", server.Name),
			Selector:           fusekiServerSelectorLabels(&server),
			Port:               server.DesiredHTTPPort(),
			SecurityProfileRef: server.Spec.SecurityProfileRef,
		}, nil
	default:
		return fusekiUITargetResolution{Status: metav1.ConditionFalse, Reason: "UnsupportedTargetKind", Message: fmt.Sprintf("FusekiUI target kind %q is not supported.", ui.Spec.TargetRef.Kind)}, nil
	}
}

func (r *FusekiUIReconciler) reconcileService(ctx context.Context, ui *fusekiv1alpha1.FusekiUI, securityProfile *fusekiv1alpha1.SecurityProfile, selector map[string]string, port int32) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: ui.ServiceName(), Namespace: ui.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = mergeStringMaps(fusekiUILabels(ui), map[string]string{"fuseki.apache.org/service-role": "ui"})
		svc.Annotations = mergeStringMaps(nil, ui.Spec.Service.Annotations)
		svc.Spec.Type = ui.DesiredServiceType()
		svc.Spec.Selector = mergeStringMaps(nil, selector)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       fusekiServicePortName(securityProfile),
			Port:       port,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(port),
		}}
		return controllerutil.SetControllerReference(ui, svc, r.Scheme)
	})
	return err
}

func fusekiUILabels(ui *fusekiv1alpha1.FusekiUI) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "fuseki-ui",
		"app.kubernetes.io/instance":   ui.Name,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/ui":         ui.Name,
	}
}

func reconcileFusekiUIIngress(ctx context.Context, c client.Client, scheme *runtime.Scheme, ui *fusekiv1alpha1.FusekiUI, port int32) error {
	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(ingressGVK)
	ingress.SetNamespace(ui.Namespace)
	ingress.SetName(ui.IngressName())
	backendPort := int64(port)

	_, err := controllerutil.CreateOrUpdate(ctx, c, ingress, func() error {
		ingress.SetLabels(mergeStringMaps(fusekiUILabels(ui), map[string]string{"fuseki.apache.org/exposure": "ingress"}))
		ingress.SetAnnotations(mergeStringMaps(nil, ui.Spec.Ingress.Annotations))

		rule := map[string]any{
			"host": ui.Spec.Ingress.Host,
			"http": map[string]any{
				"paths": []any{map[string]any{
					"path":     ui.DesiredIngressPath(),
					"pathType": "Prefix",
					"backend": map[string]any{
						"service": map[string]any{
							"name": ui.ServiceName(),
							"port": map[string]any{"number": backendPort},
						},
					},
				}},
			},
		}
		spec := map[string]any{"rules": []any{rule}}
		if ui.Spec.Ingress.ClassName != "" {
			spec["ingressClassName"] = ui.Spec.Ingress.ClassName
		}
		if ui.Spec.Ingress.TLSSecretRef != nil && ui.Spec.Ingress.TLSSecretRef.Name != "" {
			spec["tls"] = []any{map[string]any{
				"hosts":      []any{ui.Spec.Ingress.Host},
				"secretName": ui.Spec.Ingress.TLSSecretRef.Name,
			}}
		}
		if err := unstructured.SetNestedMap(ingress.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ui, ingress, scheme)
	})
	return err
}

func deleteFusekiUIIngress(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}

	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(ingressGVK)
	ingress.SetNamespace(namespace)
	ingress.SetName(name)
	err := c.Delete(ctx, ingress)
	if apimeta.IsNoMatchError(err) {
		return nil
	}
	return client.IgnoreNotFound(err)
}

func reconcileFusekiUIHTTPRoute(ctx context.Context, c client.Client, scheme *runtime.Scheme, ui *fusekiv1alpha1.FusekiUI, securityProfile *fusekiv1alpha1.SecurityProfile, port int32) error {
	httpRoute := &unstructured.Unstructured{}
	httpRoute.SetGroupVersionKind(httpRouteGVK)
	httpRoute.SetNamespace(ui.Namespace)
	httpRoute.SetName(ui.HTTPRouteName())
	backendPort := int64(port)

	_, err := controllerutil.CreateOrUpdate(ctx, c, httpRoute, func() error {
		httpRoute.SetLabels(mergeStringMaps(fusekiUILabels(ui), map[string]string{"fuseki.apache.org/exposure": "gateway"}))
		httpRoute.SetAnnotations(mergeStringMaps(nil, ui.Spec.Gateway.Annotations))

		parentRefs := make([]any, 0, len(ui.Spec.Gateway.ParentRefs))
		for _, ref := range ui.Spec.Gateway.ParentRefs {
			parentRef := map[string]any{"name": ref.Name}
			if ref.Namespace != "" {
				parentRef["namespace"] = ref.Namespace
			}
			if ref.SectionName != "" {
				parentRef["sectionName"] = ref.SectionName
			}
			parentRefs = append(parentRefs, parentRef)
		}

		spec := map[string]any{
			"parentRefs": parentRefs,
			"rules": []any{map[string]any{
				"matches": []any{map[string]any{
					"path": map[string]any{
						"type":  "PathPrefix",
						"value": ui.DesiredGatewayPath(),
					},
				}},
				"backendRefs": []any{map[string]any{
					"name": ui.ServiceName(),
					"port": backendPort,
				}},
			}},
		}
		if len(ui.Spec.Gateway.Hostnames) > 0 {
			hostnames := make([]any, 0, len(ui.Spec.Gateway.Hostnames))
			for _, hostname := range ui.Spec.Gateway.Hostnames {
				hostnames = append(hostnames, hostname)
			}
			spec["hostnames"] = hostnames
		}
		if err := unstructured.SetNestedMap(httpRoute.Object, spec, "spec"); err != nil {
			return err
		}
		if securityProfileTLSEnabled(securityProfile) {
			httpRoute.SetAnnotations(mergeStringMaps(httpRoute.GetAnnotations(), map[string]string{"fuseki.apache.org/backend-scheme": "https"}))
		}
		return controllerutil.SetControllerReference(ui, httpRoute, scheme)
	})
	return err
}

func deleteFusekiUIHTTPRoute(ctx context.Context, c client.Client, namespace, name string) error {
	if name == "" {
		return nil
	}

	httpRoute := &unstructured.Unstructured{}
	httpRoute.SetGroupVersionKind(httpRouteGVK)
	httpRoute.SetNamespace(namespace)
	httpRoute.SetName(name)
	err := c.Delete(ctx, httpRoute)
	if apimeta.IsNoMatchError(err) {
		return nil
	}
	return client.IgnoreNotFound(err)
}

func (r *FusekiUIReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForTarget(ctx, obj.GetNamespace(), fusekiv1alpha1.EndpointTargetKindFusekiCluster, obj.GetName())
}

func (r *FusekiUIReconciler) requestsForServer(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForTarget(ctx, obj.GetNamespace(), fusekiv1alpha1.EndpointTargetKindFusekiServer, obj.GetName())
}

func (r *FusekiUIReconciler) requestsForTarget(ctx context.Context, namespace string, kind fusekiv1alpha1.EndpointTargetKind, name string) []reconcile.Request {
	var uis fusekiv1alpha1.FusekiUIList
	if err := r.List(ctx, &uis, client.InNamespace(namespace)); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range uis.Items {
		ui := &uis.Items[i]
		if ui.Spec.TargetRef.Kind != kind || ui.Spec.TargetRef.Name != name {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ui)})
	}

	return requests
}

func (r *FusekiUIReconciler) requestsForSecurityProfile(ctx context.Context, obj client.Object) []reconcile.Request {
	var uis fusekiv1alpha1.FusekiUIList
	if err := r.List(ctx, &uis, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range uis.Items {
		ui := &uis.Items[i]
		securityProfileRef, err := r.resolveTargetSecurityProfileRef(ctx, ui)
		if err != nil || securityProfileRef == nil || securityProfileRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ui)})
	}

	return requests
}

func (r *FusekiUIReconciler) resolveTargetSecurityProfileRef(ctx context.Context, ui *fusekiv1alpha1.FusekiUI) (*corev1.LocalObjectReference, error) {
	switch ui.Spec.TargetRef.Kind {
	case fusekiv1alpha1.EndpointTargetKindFusekiCluster:
		var cluster fusekiv1alpha1.FusekiCluster
		if err := r.Get(ctx, client.ObjectKey{Namespace: ui.Namespace, Name: ui.Spec.TargetRef.Name}, &cluster); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		return cluster.Spec.SecurityProfileRef, nil
	case fusekiv1alpha1.EndpointTargetKindFusekiServer:
		var server fusekiv1alpha1.FusekiServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: ui.Namespace, Name: ui.Spec.TargetRef.Name}, &server); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		return server.Spec.SecurityProfileRef, nil
	default:
		return nil, nil
	}
}
