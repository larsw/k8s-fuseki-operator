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

func TestRDFDeltaServerReconcileCreatesBackupCronJobFromBackupPolicy(t *testing.T) {
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

	policy := &fusekiv1alpha1.BackupPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
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
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec: fusekiv1alpha1.RDFDeltaServerSpec{
			Image:           "ghcr.io/example/rdf-delta:latest",
			ServicePort:     1066,
			BackupPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RDFDeltaServer{}).
		WithObjects(server, policy).
		Build()

	reconciler := &RDFDeltaServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	cronJob := &batchv1.CronJob{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta-backup"}, cronJob); err != nil {
		t.Fatalf("get backup cronjob: %v", err)
	}
	if cronJob.Spec.Schedule != "0 2 * * *" {
		t.Fatalf("unexpected cronjob schedule: %q", cronJob.Spec.Schedule)
	}
	if got := cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "data-delta-0" {
		t.Fatalf("unexpected backup pvc claim: %q", got)
	}
	container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "RDF_DELTA_BACKUP_PREFIX"); got != "rdf-delta/default/delta" {
		t.Fatalf("unexpected backup prefix: %q", got)
	}
	if secretName := envVarSecretRefName(container.Env, "AWS_ACCESS_KEY_ID"); secretName != "backup-creds" {
		t.Fatalf("unexpected access key secret ref: %q", secretName)
	}
	if secretName := envVarSecretRefName(container.Env, "AWS_SECRET_ACCESS_KEY"); secretName != "backup-creds" {
		t.Fatalf("unexpected secret key secret ref: %q", secretName)
	}

	updated := &fusekiv1alpha1.RDFDeltaServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if updated.Status.BackupCronJobName != "delta-backup" {
		t.Fatalf("unexpected backup cronjob status name: %q", updated.Status.BackupCronJobName)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, backupReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "BackupCronJobReady" {
		t.Fatalf("unexpected backup condition: %#v", condition)
	}
}

func TestRDFDeltaServerReconcileScalesDownDuringActiveRestore(t *testing.T) {
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
			Replicas:    1,
		},
	}
	request := &fusekiv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "delta-restore", Namespace: "default"},
		Spec: fusekiv1alpha1.RestoreRequestSpec{
			TargetRef: fusekiv1alpha1.RestoreRequestTargetRef{Kind: fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer, Name: server.Name},
		},
		Status: fusekiv1alpha1.RestoreRequestStatus{Phase: "Running"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.RDFDeltaServer{}).
		WithObjects(server, request).
		Build()

	reconciler := &RDFDeltaServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "delta"}, statefulSet); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 0 {
		t.Fatalf("expected statefulset replicas to be scaled down during restore, got %#v", statefulSet.Spec.Replicas)
	}

	updated := &fusekiv1alpha1.RDFDeltaServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if updated.Status.ActiveRestoreName != "delta-restore" {
		t.Fatalf("unexpected active restore name: %q", updated.Status.ActiveRestoreName)
	}
	if updated.Status.Phase != "Restoring" {
		t.Fatalf("unexpected server phase: %q", updated.Status.Phase)
	}
	restoreCondition := apimeta.FindStatusCondition(updated.Status.Conditions, restoreReadyConditionType)
	if restoreCondition == nil || restoreCondition.Status != metav1.ConditionFalse || restoreCondition.Reason != "RestoreInProgress" {
		t.Fatalf("unexpected restore condition: %#v", restoreCondition)
	}
}
