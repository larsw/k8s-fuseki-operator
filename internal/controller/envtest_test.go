package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

func TestEnvtestFusekiServerDatasetBootstrap(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-server"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "primary"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "admin-auth"},
		},
	}
	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-auth", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
			OIDCIssuerURL:             "https://issuer.example.com",
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: namespace.Name},
		StringData: map[string]string{"username": "admin", "password": "secret"},
	}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:    "primary",
			Preload: []fusekiv1alpha1.DatasetPreloadSource{{URI: "https://example.org/data.ttl", Format: "text/turtle"}},
		},
	}

	for _, obj := range []ctrlclient.Object{server, profile, secret, dataset} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	datasetReconciler := &DatasetReconciler{Client: client, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(ctx, reconcileRequest(dataset)); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	serverReconciler := &FusekiServerReconciler{Client: client, Scheme: scheme}
	if _, err := serverReconciler.Reconcile(ctx, reconcileRequest(server)); err != nil {
		t.Fatalf("reconcile server: %v", err)
	}

	job := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, "server-standalone-primary-bootstrap"), job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := envVarSecretRefName(container.Env, "FUSEKI_ADMIN_PASSWORD"); got != "admin-secret" {
		t.Fatalf("unexpected password secret ref: %q", got)
	}
	if got := envVarValue(container.Env, "FUSEKI_WRITE_URL"); got != "http://standalone:3030" {
		t.Fatalf("unexpected write url: %q", got)
	}

	service := &corev1.Service{}
	if err := client.Get(ctx, objectKey(namespace.Name, server.ServiceName()), service); err != nil {
		t.Fatalf("get server service: %v", err)
	}
	if got := service.Spec.Selector["fuseki.apache.org/server"]; got != server.Name {
		t.Fatalf("unexpected server selector: %q", got)
	}
}

func TestEnvtestFusekiClusterLeaseFailover(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-cluster"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:             "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: "delta"},
		},
	}
	pod0 := readyPod(namespace.Name, cluster.Name, "example-0")
	pod1 := readyPod(namespace.Name, cluster.Name, "example-1")

	for _, obj := range []ctrlclient.Object{cluster, pod0, pod1} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}
	for _, name := range []string{pod0.Name, pod1.Name} {
		if err := markPodReady(ctx, client, namespace.Name, name); err != nil {
			t.Fatalf("mark pod ready: %v", err)
		}
	}

	reconciler := &FusekiClusterReconciler{Client: client, Scheme: scheme}
	if _, err := reconciler.Reconcile(ctx, reconcileRequest(cluster)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	lease := &coordinationv1.Lease{}
	if err := client.Get(ctx, objectKey(namespace.Name, cluster.WriteLeaseName()), lease); err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "example-0" {
		t.Fatalf("unexpected first lease holder: %v", lease.Spec.HolderIdentity)
	}

	if err := client.Delete(ctx, pod0); err != nil {
		t.Fatalf("delete pod0: %v", err)
	}
	if _, err := reconciler.Reconcile(ctx, reconcileRequest(cluster)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	if err := client.Get(ctx, objectKey(namespace.Name, cluster.WriteLeaseName()), lease); err != nil {
		t.Fatalf("get updated lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "example-1" {
		t.Fatalf("unexpected failover lease holder: %v", lease.Spec.HolderIdentity)
	}

	updatedPod := &corev1.Pod{}
	if err := client.Get(ctx, objectKey(namespace.Name, pod1.Name), updatedPod); err != nil {
		t.Fatalf("get updated pod1: %v", err)
	}
	if got := updatedPod.Labels["fuseki.apache.org/lease-holder"]; got != "true" {
		t.Fatalf("unexpected lease-holder label after failover: %q", got)
	}
}

func startEnvtestClient(t *testing.T) (*envtest.Environment, ctrlclient.Client, *runtime.Scheme) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "bases")
	if _, err := os.Stat(crdPath); err != nil {
		t.Fatalf("stat CRD directory: %v", err)
	}
	binaryDir, err := envtestBinaryDir()
	if err != nil {
		t.Skipf("envtest binaries unavailable: %v", err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	env := &envtest.Environment{CRDDirectoryPaths: []string{crdPath}, ErrorIfCRDPathMissing: true, BinaryAssetsDirectory: binaryDir}
	config, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := env.Stop(); stopErr != nil {
			t.Fatalf("stop envtest: %v", stopErr)
		}
	})

	client, err := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	return env, client, scheme
}

func envtestBinaryDir() (string, error) {
	if path := os.Getenv("KUBEBUILDER_ASSETS"); path != "" {
		if envtestAssetsPresent(path) {
			return path, nil
		}
		return "", os.ErrNotExist
	}
	defaultPath := filepath.Join(os.TempDir(), "kubebuilder", "bin")
	if envtestAssetsPresent(defaultPath) {
		return defaultPath, nil
	} else {
		return "", os.ErrNotExist
	}
}

func envtestAssetsPresent(dir string) bool {
	for _, name := range []string{"etcd", "kube-apiserver", "kubectl"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return false
		}
	}
	return true
}

func reconcileRequest(obj ctrlclient.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: ctrlclient.ObjectKeyFromObject(obj)}
}

func objectKey(namespace, name string) ctrlclient.ObjectKey {
	return ctrlclient.ObjectKey{Namespace: namespace, Name: name}
}

func readyPod(namespace, clusterName, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "fuseki",
				"fuseki.apache.org/cluster": clusterName,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "fuseki", Image: "ghcr.io/example/fuseki:6.0.0"}}},
	}
}

func markPodReady(ctx context.Context, client ctrlclient.Client, namespace, name string) error {
	pod := &corev1.Pod{}
	if err := client.Get(ctx, objectKey(namespace, name), pod); err != nil {
		return err
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return client.Status().Update(ctx, pod)
}
