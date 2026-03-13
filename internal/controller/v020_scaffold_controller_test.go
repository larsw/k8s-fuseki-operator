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

func TestSecurityProfileReconcileInvalidLocalModeWithRangerSettings(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-local", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			Authorization: &fusekiv1alpha1.SecurityAuthorizationSpec{
				Mode: fusekiv1alpha1.AuthorizationModeLocal,
				Ranger: &fusekiv1alpha1.RangerAuthorizationSpec{
					AdminURL:    "https://ranger.example.com",
					ServiceName: "fuseki-default",
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithObjects(profile).
		Build()

	reconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for invalid static spec, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(profile), updated); err != nil {
		t.Fatalf("get updated profile: %v", err)
	}
	if updated.Status.Phase != "Invalid" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "InvalidSpec" {
		t.Fatalf("expected InvalidSpec condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "authorization.ranger is not allowed") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestIngestPipelineReconcileInvalidSpec(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Source: fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeFilesystem, URI: "https://example.com/data.ttl"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Invalid" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "InvalidSpec" {
		t.Fatalf("expected InvalidSpec condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "spec.target.datasetRef.name is required") || !strings.Contains(condition.Message, "spec.shaclPolicyRef.name is required") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestSecurityPolicyReconcileReadyWhenTargetsResolved(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.SecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-policy", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityPolicySpec{
			Rules: []fusekiv1alpha1.SecurityPolicyRule{{
				Target:     fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "example-dataset"}},
				Actions:    []fusekiv1alpha1.SecurityPolicyAction{fusekiv1alpha1.SecurityPolicyActionQuery},
				Subjects:   []fusekiv1alpha1.SecuritySubject{{Type: fusekiv1alpha1.SecuritySubjectTypeUser, Value: "alice"}},
				Expression: "PUBLIC",
			}},
		},
	}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "example-policy"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityPolicy{}).
		WithObjects(policy, dataset).
		Build()

	reconciler := &SecurityPolicyReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.SecurityPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "DependenciesResolved" {
		t.Fatalf("expected DependenciesResolved condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "referenced by 1 Dataset") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestSecurityPolicyReconcilePendingWhenTargetDatasetMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.SecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-policy", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityPolicySpec{
			Rules: []fusekiv1alpha1.SecurityPolicyRule{{
				Target:     fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "missing-dataset"}},
				Actions:    []fusekiv1alpha1.SecurityPolicyAction{fusekiv1alpha1.SecurityPolicyActionQuery},
				Subjects:   []fusekiv1alpha1.SecuritySubject{{Type: fusekiv1alpha1.SecuritySubjectTypeUser, Value: "alice"}},
				Expression: "PUBLIC",
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityPolicy{}).
		WithObjects(policy).
		Build()

	reconciler := &SecurityPolicyReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.SecurityPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "DependenciesMissing" {
		t.Fatalf("expected DependenciesMissing condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "missing-dataset") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}
