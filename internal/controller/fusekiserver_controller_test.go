package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestFusekiServerReconcileCreatesBaseResources(t *testing.T) {
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

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "dataset-a", Namespace: "default"},
		Spec:       fusekiv1alpha1.DatasetSpec{Name: "primary"},
	}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "dataset-a"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "admin-auth"},
		},
	}
	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-auth", Namespace: "default"},
		Spec:       fusekiv1alpha1.SecurityProfileSpec{AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiServer{}).
		WithObjects(server, dataset, profile).
		Build()

	datasetReconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	reconciler := &FusekiServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone-config"}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if got := configMap.Data["mode"]; got != "single-server" {
		t.Fatalf("unexpected mode: %q", got)
	}

	service := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone"}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if got := service.Spec.Ports[0].Port; got != 3030 {
		t.Fatalf("unexpected service port: %d", got)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone-data"}, pvc); err != nil {
		t.Fatalf("get pvc: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone"}, deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := deployment.Spec.Template.Spec.Containers[0].Args[1]; got != "/fuseki-extra/operator-config/run-fuseki.sh" {
		t.Fatalf("unexpected container startup script: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "FUSEKI_DATASET_CONFIG_DIR"); got != "" {
		t.Fatalf("expected no legacy dataset config dir env var, got %q", got)
	}
	if secretName := envVarSecretRefName(deployment.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); secretName != "admin-secret" {
		t.Fatalf("unexpected deployment admin password secret: %q", secretName)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "server-standalone-dataset-a-bootstrap"}, job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_WRITE_URL"); got != "http://standalone:3030" {
		t.Fatalf("unexpected job write url: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if updated.Status.ServiceName != "standalone" {
		t.Fatalf("unexpected status service name: %q", updated.Status.ServiceName)
	}
	if updated.Status.DeploymentName != "standalone" {
		t.Fatalf("unexpected status deployment name: %q", updated.Status.DeploymentName)
	}
}
