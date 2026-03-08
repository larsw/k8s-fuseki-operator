package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
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

func TestRestoreRequestReconcileWaitsForScaleDown(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	policy := readyBackupPolicy("nightly")
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec: fusekiv1alpha1.RDFDeltaServerSpec{
			Image:           "ghcr.io/example/rdf-delta:latest",
			ServicePort:     1066,
			BackupPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	request := &fusekiv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "default"},
		Spec: fusekiv1alpha1.RestoreRequestSpec{
			TargetRef: fusekiv1alpha1.RestoreRequestTargetRef{Kind: fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer, Name: server.Name},
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: server.StatefulSetName(), Namespace: server.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptrTo(int32(1))},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RestoreRequest{}).
		WithObjects(policy, server, request, statefulSet).
		Build()

	reconciler := &RestoreRequestReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != backupPolicyRequeueInterval {
		t.Fatalf("expected %s requeue, got %s", backupPolicyRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.RestoreRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated request: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, restoreCompletedConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ScalingDown" {
		t.Fatalf("unexpected restore condition: %#v", condition)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: request.JobName()}, job); err == nil {
		t.Fatalf("expected no restore job before scale down completes")
	}
}

func TestRestoreRequestReconcileCreatesRestoreJobWhenScaledDown(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	policy := readyBackupPolicy("nightly")
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec: fusekiv1alpha1.RDFDeltaServerSpec{
			Image:           "ghcr.io/example/rdf-delta:latest",
			ServicePort:     1066,
			BackupPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	request := &fusekiv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "default"},
		Spec: fusekiv1alpha1.RestoreRequestSpec{
			TargetRef:    fusekiv1alpha1.RestoreRequestTargetRef{Kind: fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer, Name: server.Name},
			BackupObject: "20260308T120000Z-delta.tgz",
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: server.StatefulSetName(), Namespace: server.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptrTo(int32(0))},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RestoreRequest{}).
		WithObjects(policy, server, request, statefulSet).
		Build()

	reconciler := &RestoreRequestReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: request.JobName()}, job); err != nil {
		t.Fatalf("get restore job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "BACKUP_OBJECT"); got != "20260308T120000Z-delta.tgz" {
		t.Fatalf("unexpected backup object env: %q", got)
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "data-delta-0" {
		t.Fatalf("unexpected restore pvc claim: %q", got)
	}

	updated := &fusekiv1alpha1.RestoreRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated request: %v", err)
	}
	if updated.Status.ResolvedBackupRef != "20260308T120000Z-delta.tgz" {
		t.Fatalf("unexpected resolved backup ref: %q", updated.Status.ResolvedBackupRef)
	}
	if updated.Status.JobName != request.JobName() {
		t.Fatalf("unexpected job name: %q", updated.Status.JobName)
	}
}

func TestRestoreRequestReconcileKeepsCompletedRestoreTerminal(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}

	policy := readyBackupPolicy("nightly")
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec: fusekiv1alpha1.RDFDeltaServerSpec{
			Image:           "ghcr.io/example/rdf-delta:latest",
			ServicePort:     1066,
			BackupPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	request := &fusekiv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "default"},
		Spec: fusekiv1alpha1.RestoreRequestSpec{
			TargetRef:    fusekiv1alpha1.RestoreRequestTargetRef{Kind: fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer, Name: server.Name},
			BackupObject: "20260308T120000Z-delta",
		},
		Status: fusekiv1alpha1.RestoreRequestStatus{
			ObservedGeneration: 1,
			Phase:              "Succeeded",
			TargetName:         server.Name,
			JobName:            "restore-restore",
			ResolvedBackupRef:  "20260308T120000Z-delta",
			Conditions: []metav1.Condition{{
				Type:   restoreCompletedConditionType,
				Status: metav1.ConditionTrue,
				Reason: "RestoreCompleted",
			}},
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: server.StatefulSetName(), Namespace: server.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptrTo(int32(1))},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RestoreRequest{}).
		WithObjects(policy, server, request, statefulSet).
		Build()

	reconciler := &RestoreRequestReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result != (ctrl.Result{}) {
		t.Fatalf("expected no requeue for completed restore, got %#v", result)
	}

	updated := &fusekiv1alpha1.RestoreRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated request: %v", err)
	}
	if updated.Status.Phase != "Succeeded" {
		t.Fatalf("unexpected phase after terminal reconcile: %q", updated.Status.Phase)
	}
}

func readyBackupPolicy(name string) *fusekiv1alpha1.BackupPolicy {
	return &fusekiv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: fusekiv1alpha1.BackupPolicySpec{
			Schedule: "0 2 * * *",
			S3: fusekiv1alpha1.BackupPolicyS3Spec{
				Endpoint: "https://minio.example.test",
				Bucket:   "fuseki-backups",
				Prefix:   "rdf-delta",
				CredentialsSecretRef: corev1.LocalObjectReference{
					Name: "backup-creds",
				},
			},
		},
		Status: fusekiv1alpha1.BackupPolicyStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: backupPolicyConfiguredReason}},
		},
	}
}
