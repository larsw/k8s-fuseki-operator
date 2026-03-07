package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestEndpointReconcileCreatesClusterServices(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	cluster := &fusekiv1alpha1.FusekiCluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiClusterSpec{Image: "ghcr.io/example/fuseki:6.0.0", RDFDeltaServerRef: corev1.LocalObjectReference{Name: "delta"}}}
	endpoint := &fusekiv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: "default"},
		Spec: fusekiv1alpha1.EndpointSpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiCluster, Name: "example"},
			Read:      fusekiv1alpha1.EndpointServiceSpec{Annotations: map[string]string{"exposure": "read"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Endpoint{}).
		WithObjects(cluster, endpoint).
		Build()

	reconciler := &EndpointReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(endpoint)}); err != nil {
		t.Fatalf("reconcile endpoint: %v", err)
	}

	readService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "public-read"}, readService); err != nil {
		t.Fatalf("get read service: %v", err)
	}
	if got := readService.Spec.Selector["fuseki.apache.org/read-route"]; got != "true" {
		t.Fatalf("unexpected read selector: %q", got)
	}

	writeService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "public-write"}, writeService); err != nil {
		t.Fatalf("get write service: %v", err)
	}
	if got := writeService.Spec.Selector["fuseki.apache.org/write-route"]; got != "true" {
		t.Fatalf("unexpected write selector: %q", got)
	}

	updated := &fusekiv1alpha1.Endpoint{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(endpoint), updated); err != nil {
		t.Fatalf("get updated endpoint: %v", err)
	}
	if updated.Status.ReadServiceName != "public-read" || updated.Status.WriteServiceName != "public-write" {
		t.Fatalf("unexpected endpoint status services: %#v", updated.Status)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected configured condition true, got %#v", condition)
	}
}
