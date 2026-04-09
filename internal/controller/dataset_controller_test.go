package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
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

	if got := configMap.Data["spatial.properties"]; got == "" || containsLine(got, "spatial.enabled=true") {
		t.Fatalf("expected spatial properties without duplicate enabled flag, got %q", got)
	}
	if got := configMap.Data["dataset.properties"]; got == "" || !containsLine(got, "spatial.enabled=true") || containsLine(got, "write.url=http://") || containsLine(got, "target.name=") {
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

func TestDatasetReconcileBundlesSecurityPolicies(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "secured-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "example-securitypolicy"}},
		},
	}
	policy := &fusekiv1alpha1.SecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-securitypolicy", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityPolicySpec{
			Rules: []fusekiv1alpha1.SecurityPolicyRule{{
				Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "secured-dataset"}},
				Actions: []fusekiv1alpha1.SecurityPolicyAction{
					fusekiv1alpha1.SecurityPolicyActionQuery,
				},
				Subjects: []fusekiv1alpha1.SecuritySubject{{
					Type:  fusekiv1alpha1.SecuritySubjectTypeGroup,
					Value: "analysts",
				}},
				Expression: "PUBLIC",
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithObjects(dataset, policy).
		Build()

	reconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: dataset.ConfigMapName()}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}

	bundle := configMap.Data["security-policies.json"]
	if !strings.Contains(bundle, `"name": "example-securitypolicy"`) || !strings.Contains(bundle, `"effect": "Allow"`) || !strings.Contains(bundle, `"expressionType": "Simple"`) {
		t.Fatalf("unexpected security policy bundle: %q", bundle)
	}
	if _, exists := configMap.Data["security-policies.missing"]; exists {
		t.Fatalf("did not expect missing-policy marker when all policies resolve")
	}

	updated := &fusekiv1alpha1.Dataset{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(dataset), updated); err != nil {
		t.Fatalf("get updated dataset: %v", err)
	}
	if updated.Status.Phase != "Defined" {
		t.Fatalf("unexpected dataset phase: %q", updated.Status.Phase)
	}
}

func TestDatasetReconcilePendingWhenSecurityPolicyMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "secured-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "missing-policy"}},
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
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected dataset phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "SecurityPoliciesMissing" {
		t.Fatalf("expected SecurityPoliciesMissing condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "missing-policy") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: dataset.ConfigMapName()}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if got := configMap.Data["security-policies.missing"]; !strings.Contains(got, "missing-policy") {
		t.Fatalf("unexpected missing-policy marker: %q", got)
	}
	if _, exists := configMap.Data["security-policies.json"]; exists {
		t.Fatalf("did not expect resolved security policy bundle when references are missing")
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

func volumeMountPath(mounts []corev1.VolumeMount, name string) string {
	for _, mount := range mounts {
		if mount.Name == name {
			return mount.MountPath
		}
	}
	return ""
}

func configMapVolumeName(volumes []corev1.Volume, name string) string {
	for _, volume := range volumes {
		if volume.Name == name && volume.ConfigMap != nil {
			return volume.ConfigMap.Name
		}
	}
	return ""
}

func secretVolumeName(volumes []corev1.Volume, name string) string {
	for _, volume := range volumes {
		if volume.Name == name && volume.Secret != nil {
			return volume.Secret.SecretName
		}
	}
	return ""
}

func projectedConfigMapPaths(volumes []corev1.Volume, name string) map[string]string {
	paths := map[string]string{}
	for _, volume := range volumes {
		if volume.Name != name || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.ConfigMap == nil {
				continue
			}
			for _, item := range source.ConfigMap.Items {
				paths[item.Key] = item.Path
			}
		}
	}
	return paths
}

func containsLine(content, needle string) bool {
	for _, line := range strings.Split(content, "\n") {
		if line == needle {
			return true
		}
	}
	return false
}
