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

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestBackupPolicyReconcileReadyWhenScheduleAndSecretAreValid(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-creds", Namespace: "default"},
		Data: map[string][]byte{
			backupPolicyAccessKeyKey: []byte("minio"),
			backupPolicySecretKeyKey: []byte("secret123"),
		},
	}
	policy := &fusekiv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: fusekiv1alpha1.BackupPolicySpec{
			Schedule: "0 1 * * *",
			S3: fusekiv1alpha1.BackupPolicyS3Spec{
				Endpoint: "https://minio.example.test",
				Bucket:   "fuseki-backups",
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: secret.Name,
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.BackupPolicy{}).
		WithObjects(secret, policy).
		Build()

	reconciler := &BackupPolicyReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.BackupPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected policy phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != backupPolicyConfiguredReason {
		t.Fatalf("unexpected configured condition: %#v", condition)
	}
}

func TestBackupPolicyReconcilePendingWhenSecretMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: fusekiv1alpha1.BackupPolicySpec{
			Schedule: "0 1 * * *",
			S3: fusekiv1alpha1.BackupPolicyS3Spec{
				Endpoint: "https://minio.example.test",
				Bucket:   "fuseki-backups",
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: "backup-creds",
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.BackupPolicy{}).
		WithObjects(policy).
		Build()

	reconciler := &BackupPolicyReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != backupPolicyRequeueInterval {
		t.Fatalf("expected %s requeue, got %s", backupPolicyRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.BackupPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected policy phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ReferencesMissing" {
		t.Fatalf("unexpected configured condition: %#v", condition)
	}
}

func TestBackupPolicyReconcilePendingWhenScheduleIsInvalid(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-creds", Namespace: "default"},
		Data: map[string][]byte{
			backupPolicyAccessKeyKey: []byte("minio"),
			backupPolicySecretKeyKey: []byte("secret123"),
		},
	}
	policy := &fusekiv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: fusekiv1alpha1.BackupPolicySpec{
			Schedule: "not-a-cron",
			S3: fusekiv1alpha1.BackupPolicyS3Spec{
				Endpoint: "https://minio.example.test",
				Bucket:   "fuseki-backups",
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: secret.Name,
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.BackupPolicy{}).
		WithObjects(secret, policy).
		Build()

	reconciler := &BackupPolicyReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.BackupPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ScheduleInvalid" {
		t.Fatalf("unexpected configured condition: %#v", condition)
	}
}
