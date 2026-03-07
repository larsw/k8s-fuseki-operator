package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestDatasetReconcileCreatesGenericConfigMap(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:    "primary",
			Preload: []fusekiv1alpha1.DatasetPreloadSource{{URI: "https://example.org/data.ttl"}},
			Spatial: &fusekiv1alpha1.JenaSpatialSpec{Enabled: true, Assembler: "spatial:EntityMap"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithObjects(dataset).
		Build()

	reconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-dataset-dataset-config"}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}

	if got := configMap.Data["spatial.properties"]; got == "" {
		t.Fatalf("expected spatial properties to be rendered")
	}
	if got := configMap.Data["dataset.properties"]; got == "" || containsLine(got, "write.url=http://") || containsLine(got, "target.name=") {
		t.Fatalf("expected generic dataset properties without target binding, got %q", got)
	}

	updated := &fusekiv1alpha1.Dataset{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(dataset), updated); err != nil {
		t.Fatalf("get updated dataset: %v", err)
	}

	if updated.Status.ConfigMapName != "example-dataset-dataset-config" {
		t.Fatalf("unexpected configmap name: %q", updated.Status.ConfigMapName)
	}
}

func TestDatasetReconcileStoresDefinedPhase(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "dataset-defined", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:        "primary",
			DisplayName: "Primary dataset",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithObjects(dataset).
		Build()

	reconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.Dataset{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(dataset), updated); err != nil {
		t.Fatalf("get updated dataset: %v", err)
	}
	if updated.Status.Phase != "Defined" {
		t.Fatalf("unexpected dataset phase: %q", updated.Status.Phase)
	}
}

func TestDatasetBootstrapContainerWaitsForWriteService(t *testing.T) {
	t.Helper()

	container := datasetBootstrapContainer(
		&fusekiv1alpha1.Dataset{Spec: fusekiv1alpha1.DatasetSpec{Name: "primary", Type: fusekiv1alpha1.DatasetTypeTDB2}},
		datasetBootstrapTarget{Kind: "cluster", Name: "example", Image: "fuseki:test", WriteURL: "http://example-write:3030"},
		nil,
		nil,
	)

	if len(container.Command) < 3 {
		t.Fatalf("unexpected bootstrap command: %#v", container.Command)
	}

	script := container.Command[2]
	if !strings.Contains(script, "${FUSEKI_WRITE_URL}/$/ping") {
		t.Fatalf("expected bootstrap script to probe write service readiness, got %q", script)
	}
	if !strings.Contains(script, "Fuseki write service did not become ready in time") {
		t.Fatalf("expected bootstrap script timeout guard, got %q", script)
	}
}

func envVarValue(env []corev1.EnvVar, name string) string {
	for _, entry := range env {
		if entry.Name == name {
			return entry.Value
		}
	}
	return ""
}

func envVarSecretRefName(env []corev1.EnvVar, name string) string {
	for _, entry := range env {
		if entry.Name == name && entry.ValueFrom != nil && entry.ValueFrom.SecretKeyRef != nil {
			return entry.ValueFrom.SecretKeyRef.Name
		}
	}
	return ""
}

func containsLine(content, needle string) bool {
	for _, line := range strings.Split(content, "\n") {
		if line == needle {
			return true
		}
	}
	return false
}
