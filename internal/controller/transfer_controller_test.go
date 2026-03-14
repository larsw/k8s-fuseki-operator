package controller

import (
	"context"
	"strings"
	"testing"

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

func TestImportRequestReconcileCreatesTransferJobForFusekiServer(t *testing.T) {
	t.Helper()

	scheme := newTransferTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	request := &fusekiv1alpha1.ImportRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "example-import", Namespace: "default"},
		Spec: fusekiv1alpha1.ImportRequestSpec{
			Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source: fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl", Format: "text/turtle"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ImportRequest{}).
		WithObjects(dataset, server, request).
		Build()

	reconciler := &ImportRequestReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatalf("reconcile import request: %v", err)
	}

	updated := &fusekiv1alpha1.ImportRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated import request: %v", err)
	}
	if updated.Status.JobName != request.JobName() {
		t.Fatalf("unexpected job name: %q", updated.Status.JobName)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected configured condition true, got %#v", condition)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: request.Namespace, Name: request.JobName()}, job); err != nil {
		t.Fatalf("get import job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != server.Spec.Image {
		t.Fatalf("unexpected job image: %q", container.Image)
	}
	if got := envVarValue(container.Env, "FUSEKI_IMPORT_URL"); got != "http://standalone:3030/primary/data" {
		t.Fatalf("unexpected import URL: %q", got)
	}
	if got := envVarValue(container.Env, "TRANSFER_SOURCE_URI"); got != "https://example.com/data.ttl" {
		t.Fatalf("unexpected source URI: %q", got)
	}
	if !strings.Contains(container.Command[2], "curl_fuseki") {
		t.Fatalf("expected transfer script to use curl_fuseki, got %q", container.Command[2])
	}
}

func TestImportRequestReconcilePendingWhenDatasetTargetAmbiguous(t *testing.T) {
	t.Helper()

	scheme := newTransferTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	cluster := &fusekiv1alpha1.FusekiCluster{ObjectMeta: metav1.ObjectMeta{Name: "clustered", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiClusterSpec{Image: "ghcr.io/example/fuseki:6.0.0", RDFDeltaServerRef: corev1.LocalObjectReference{Name: "delta"}, DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	request := &fusekiv1alpha1.ImportRequest{ObjectMeta: metav1.ObjectMeta{Name: "example-import", Namespace: "default"}, Spec: fusekiv1alpha1.ImportRequestSpec{Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}}, Source: fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"}}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ImportRequest{}).
		WithObjects(dataset, server, cluster, request).
		Build()

	reconciler := &ImportRequestReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatalf("reconcile import request: %v", err)
	}

	updated := &fusekiv1alpha1.ImportRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated import request: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "DatasetTargetAmbiguous" {
		t.Fatalf("expected DatasetTargetAmbiguous condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "FusekiCluster/clustered") || !strings.Contains(condition.Message, "FusekiServer/standalone") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: request.Namespace, Name: request.JobName()}, job); err == nil {
		t.Fatalf("expected no job to be created for ambiguous dataset target")
	}
}

func TestExportRequestReconcilePendingWhenS3SecretMissing(t *testing.T) {
	t.Helper()

	scheme := newTransferTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	request := &fusekiv1alpha1.ExportRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "example-export", Namespace: "default"},
		Spec: fusekiv1alpha1.ExportRequestSpec{
			Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:   fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeS3, URI: "s3://example-bucket/exports/example-dataset.nq.gz", SecretRef: &corev1.LocalObjectReference{Name: "missing-s3"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ExportRequest{}).
		WithObjects(dataset, server, request).
		Build()

	reconciler := &ExportRequestReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatalf("reconcile export request: %v", err)
	}

	updated := &fusekiv1alpha1.ExportRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated export request: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "TransferSecretNotFound" {
		t.Fatalf("expected TransferSecretNotFound condition, got %#v", condition)
	}
	if updated.Status.ArtifactRef != "s3://example-bucket/exports/example-dataset.nq.gz" {
		t.Fatalf("unexpected artifact ref: %q", updated.Status.ArtifactRef)
	}
}

func TestExportRequestReconcileCreatesTransferJobAndArtifactRef(t *testing.T) {
	t.Helper()

	scheme := newTransferTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	request := &fusekiv1alpha1.ExportRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "example-export", Namespace: "default"},
		Spec: fusekiv1alpha1.ExportRequestSpec{
			Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:   fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/", Compression: "gzip"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ExportRequest{}).
		WithObjects(dataset, server, request).
		Build()

	reconciler := &ExportRequestReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatalf("reconcile export request: %v", err)
	}

	updated := &fusekiv1alpha1.ExportRequest{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(request), updated); err != nil {
		t.Fatalf("get updated export request: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if updated.Status.ArtifactRef != "/exports/example-export.nq.gz" {
		t.Fatalf("unexpected artifact ref: %q", updated.Status.ArtifactRef)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: request.Namespace, Name: request.JobName()}, job); err != nil {
		t.Fatalf("get export job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "FUSEKI_EXPORT_URL"); got != "http://standalone:3030/primary/data" {
		t.Fatalf("unexpected export URL: %q", got)
	}
	if got := envVarValue(container.Env, "TRANSFER_ARTIFACT_REF"); got != "/exports/example-export.nq.gz" {
		t.Fatalf("unexpected transfer artifact ref: %q", got)
	}
	if !strings.Contains(container.Command[2], "Accept: ${TRANSFER_SINK_MEDIA_TYPE}") {
		t.Fatalf("expected export script to set Accept header, got %q", container.Command[2])
	}
}

func newTransferTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	return scheme
}
