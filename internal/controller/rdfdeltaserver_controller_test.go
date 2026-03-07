package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestRDFDeltaServerReconcileCreatesServiceAndStatefulSet(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec: fusekiv1alpha1.RDFDeltaServerSpec{
			Image:       "ghcr.io/example/rdf-delta:latest",
			ServicePort: 1066,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RDFDeltaServer{}).
		WithObjects(server).
		Build()

	reconciler := &RDFDeltaServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	service := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta"}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}

	if got := service.Spec.Ports[0].Port; got != 1066 {
		t.Fatalf("unexpected service port: %d", got)
	}

	headless := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta-headless"}, headless); err != nil {
		t.Fatalf("get headless service: %v", err)
	}

	if headless.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless service, got clusterIP=%q", headless.Spec.ClusterIP)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta-config"}, configMap); err != nil {
		t.Fatalf("get config map: %v", err)
	}

	if got := configMap.Data["servicePort"]; got != "1066" {
		t.Fatalf("unexpected config map servicePort: %q", got)
	}

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta"}, statefulSet); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}

	if statefulSet.Spec.ServiceName != "delta-headless" {
		t.Fatalf("unexpected statefulset service name: %q", statefulSet.Spec.ServiceName)
	}

	if len(statefulSet.Spec.Template.Spec.Volumes) != 1 {
		t.Fatalf("expected config volume on statefulset, got %d", len(statefulSet.Spec.Template.Spec.Volumes))
	}

	updated := &fusekiv1alpha1.RDFDeltaServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}

	if updated.Status.ServiceName != "delta" {
		t.Fatalf("unexpected status service name: %q", updated.Status.ServiceName)
	}

	if updated.Status.ConfigMapName != "delta-config" {
		t.Fatalf("unexpected config map status name: %q", updated.Status.ConfigMapName)
	}
}
