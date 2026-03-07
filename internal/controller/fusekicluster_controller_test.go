package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestFusekiClusterReconcileCreatesBaseResources(t *testing.T) {
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
	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-auth", Namespace: "default"},
		Spec:       fusekiv1alpha1.SecurityProfileSpec{AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"}},
	}

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "dataset-a", Namespace: "default"},
		Spec:       fusekiv1alpha1.DatasetSpec{Name: "primary"},
	}
	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef:  corev1.LocalObjectReference{Name: "delta"},
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "dataset-a"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "admin-auth"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":    "fuseki",
				"fuseki.apache.org/cluster": "example",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiCluster{}).
		WithObjects(cluster, pod, dataset, profile).
		Build()

	datasetReconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	reconciler := &FusekiClusterReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-config"}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}

	if got := configMap.Data["rdfDeltaServerRef"]; got != "delta" {
		t.Fatalf("unexpected rdf delta reference: %q", got)
	}

	writeService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-write"}, writeService); err != nil {
		t.Fatalf("get write service: %v", err)
	}

	if got := writeService.Spec.Selector["fuseki.apache.org/write-route"]; got != "true" {
		t.Fatalf("unexpected write selector route flag: %q", got)
	}

	lease := &coordinationv1.Lease{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-write"}, lease); err != nil {
		t.Fatalf("get write lease: %v", err)
	}

	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "example-0" {
		t.Fatalf("unexpected lease holder: %v", lease.Spec.HolderIdentity)
	}

	readService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-read"}, readService); err != nil {
		t.Fatalf("get read service: %v", err)
	}

	if got := readService.Spec.Selector["fuseki.apache.org/read-route"]; got != "true" {
		t.Fatalf("unexpected read selector route flag: %q", got)
	}

	headlessService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-headless"}, headlessService); err != nil {
		t.Fatalf("get headless service: %v", err)
	}

	if headlessService.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless service, got clusterIP=%q", headlessService.Spec.ClusterIP)
	}

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example"}, statefulSet); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}

	if statefulSet.Spec.ServiceName != "example-headless" {
		t.Fatalf("unexpected statefulset service name: %q", statefulSet.Spec.ServiceName)
	}

	updatedPod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-0"}, updatedPod); err != nil {
		t.Fatalf("get updated pod: %v", err)
	}

	if got := updatedPod.Labels["fuseki.apache.org/lease-holder"]; got != "true" {
		t.Fatalf("unexpected lease-holder label: %q", got)
	}

	if len(statefulSet.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected one volume claim template, got %d", len(statefulSet.Spec.VolumeClaimTemplates))
	}
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "FUSEKI_DATASET_CONFIG_DIR"); got != "" {
		t.Fatalf("expected no legacy dataset config dir env var, got %q", got)
	}
	if secretName := envVarSecretRefName(statefulSet.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); secretName != "admin-secret" {
		t.Fatalf("unexpected statefulset admin password secret: %q", secretName)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "cluster-example-dataset-a-bootstrap"}, job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_WRITE_URL"); got != "http://example-write:3030" {
		t.Fatalf("unexpected job write url: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatalf("get updated cluster: %v", err)
	}

	if updated.Status.ConfigMapName != "example-config" {
		t.Fatalf("unexpected config map status name: %q", updated.Status.ConfigMapName)
	}

	if updated.Status.StatefulSetName != "example" {
		t.Fatalf("unexpected statefulset status name: %q", updated.Status.StatefulSetName)
	}

	if updated.Status.ActiveWritePod != "example-0" {
		t.Fatalf("unexpected active write pod: %q", updated.Status.ActiveWritePod)
	}
}
