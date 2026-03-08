package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestFusekiUIReconcileCreatesClusterWriteService(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secured", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			TLSSecretRef: &corev1.LocalObjectReference{Name: "tls-secret"},
		},
		Status: fusekiv1alpha1.SecurityProfileStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: "ReferencesResolved"}},
		},
	}
	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef:  corev1.LocalObjectReference{Name: "delta"},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "secured"},
		},
	}
	ui := &fusekiv1alpha1.FusekiUI{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ui", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiUISpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiCluster, Name: "example"},
			Service: fusekiv1alpha1.EndpointServiceSpec{
				Name:        "public-web",
				Type:        corev1.ServiceTypeNodePort,
				Annotations: map[string]string{"exposure": "browser"},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiUI{}).
		WithObjects(profile, cluster, ui).
		Build()

	reconciler := &FusekiUIReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ui)}); err != nil {
		t.Fatalf("reconcile fusekiui: %v", err)
	}

	service := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "public-web"}, service); err != nil {
		t.Fatalf("get ui service: %v", err)
	}
	if got := service.Spec.Selector["fuseki.apache.org/write-route"]; got != "true" {
		t.Fatalf("unexpected ui selector: %q", got)
	}
	if got := service.Spec.Type; got != corev1.ServiceTypeNodePort {
		t.Fatalf("unexpected ui service type: %q", got)
	}
	if got := service.Spec.Ports[0].Name; got != "https" {
		t.Fatalf("unexpected ui service port name: %q", got)
	}
	if got := service.Annotations["exposure"]; got != "browser" {
		t.Fatalf("unexpected ui service annotation: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiUI{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(ui), updated); err != nil {
		t.Fatalf("get updated fusekiui: %v", err)
	}
	if updated.Status.ServiceName != "public-web" {
		t.Fatalf("unexpected ui status service name: %q", updated.Status.ServiceName)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected ui phase: %q", updated.Status.Phase)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected configured condition true, got %#v", condition)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, securityReadyConditionType); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected security condition true, got %#v", condition)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingressReadyConditionType); condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "IngressNotConfigured" {
		t.Fatalf("expected ingress condition to reflect no ingress, got %#v", condition)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, gatewayReadyConditionType); condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "GatewayNotConfigured" {
		t.Fatalf("expected gateway condition to reflect no route, got %#v", condition)
	}
}

func TestFusekiUIReconcileCreatesIngressAndGatewayRoute(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secured", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			TLSSecretRef: &corev1.LocalObjectReference{Name: "tls-secret"},
		},
		Status: fusekiv1alpha1.SecurityProfileStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: "ReferencesResolved"}},
		},
	}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "secured"},
		},
	}
	ui := &fusekiv1alpha1.FusekiUI{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ui", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiUISpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiServer, Name: "standalone"},
			Ingress: &fusekiv1alpha1.FusekiUIIngressSpec{
				Host:        "fuseki.example.test",
				ClassName:   "nginx",
				Annotations: map[string]string{"nginx.ingress.kubernetes.io/backend-protocol": "HTTPS"},
				TLSSecretRef: &corev1.LocalObjectReference{
					Name: "ui-cert",
				},
			},
			Gateway: &fusekiv1alpha1.FusekiUIGatewaySpec{
				ParentRefs: []fusekiv1alpha1.FusekiUIGatewayParentRef{{Name: "shared-gateway", Namespace: "infra", SectionName: "https"}},
				Hostnames:  []string{"fuseki.example.test"},
				Annotations: map[string]string{
					"gateway.networking.k8s.io/policy": "ui",
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiUI{}).
		WithObjects(profile, server, ui).
		Build()

	reconciler := &FusekiUIReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ui)}); err != nil {
		t.Fatalf("reconcile fusekiui exposure: %v", err)
	}

	ingress := &unstructured.Unstructured{}
	ingress.SetGroupVersionKind(ingressGVK)
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: ui.IngressName()}, ingress); err != nil {
		t.Fatalf("get ingress: %v", err)
	}
	if got, found, err := unstructured.NestedString(ingress.Object, "spec", "ingressClassName"); err != nil || !found || got != "nginx" {
		t.Fatalf("unexpected ingress class: found=%t got=%q err=%v", found, got, err)
	}
	rules, found, err := unstructured.NestedSlice(ingress.Object, "spec", "rules")
	if err != nil || !found || len(rules) != 1 {
		t.Fatalf("unexpected ingress rules: found=%t len=%d err=%v", found, len(rules), err)
	}
	rule, ok := rules[0].(map[string]any)
	if !ok || rule["host"] != "fuseki.example.test" {
		t.Fatalf("unexpected ingress rule: %#v", rules[0])
	}

	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(httpRouteGVK)
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: ui.HTTPRouteName()}, route); err != nil {
		t.Fatalf("get httproute: %v", err)
	}
	parentRefs, found, err := unstructured.NestedSlice(route.Object, "spec", "parentRefs")
	if err != nil || !found || len(parentRefs) != 1 {
		t.Fatalf("unexpected parentRefs: found=%t len=%d err=%v", found, len(parentRefs), err)
	}
	parentRef, ok := parentRefs[0].(map[string]any)
	if !ok || parentRef["name"] != "shared-gateway" || parentRef["namespace"] != "infra" || parentRef["sectionName"] != "https" {
		t.Fatalf("unexpected parentRef: %#v", parentRefs[0])
	}
	if got := route.GetAnnotations()["fuseki.apache.org/backend-scheme"]; got != "https" {
		t.Fatalf("unexpected route backend scheme annotation: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiUI{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(ui), updated); err != nil {
		t.Fatalf("get updated fusekiui: %v", err)
	}
	if updated.Status.IngressName != ui.IngressName() {
		t.Fatalf("unexpected ingress status name: %q", updated.Status.IngressName)
	}
	if updated.Status.HTTPRouteName != ui.HTTPRouteName() {
		t.Fatalf("unexpected route status name: %q", updated.Status.HTTPRouteName)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingressReadyConditionType); condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "IngressReady" {
		t.Fatalf("unexpected ingress condition: %#v", condition)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, gatewayReadyConditionType); condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "GatewayReady" {
		t.Fatalf("unexpected gateway condition: %#v", condition)
	}
}

func TestFusekiUIReconcilePendingWhenTargetMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	ui := &fusekiv1alpha1.FusekiUI{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ui", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiUISpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiServer, Name: "missing"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiUI{}).
		WithObjects(ui).
		Build()

	reconciler := &FusekiUIReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ui)})
	if err != nil {
		t.Fatalf("reconcile fusekiui: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("expected %s requeue, got %s", securityProfileRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.FusekiUI{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(ui), updated); err != nil {
		t.Fatalf("get updated fusekiui: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected ui phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "TargetNotFound" {
		t.Fatalf("unexpected configured condition: %#v", condition)
	}

	service := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "public-ui"}, service); err == nil {
		t.Fatalf("expected no ui service when target is missing")
	}
}
