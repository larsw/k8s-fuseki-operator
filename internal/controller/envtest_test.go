package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
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
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
			OIDCIssuerURL:             "https://issuer.example.com",
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: namespace.Name},
		StringData: map[string]string{"username": "admin", "password": "secret"},
	}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: namespace.Name}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: []byte("cert"), corev1.TLSPrivateKeyKey: []byte("key")}}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:    "primary",
			Preload: []fusekiv1alpha1.DatasetPreloadSource{{URI: "https://example.org/data.ttl", Format: "text/turtle"}},
		},
	}

	for _, obj := range []ctrlclient.Object{server, profile, secret, tlsSecret, dataset} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	datasetReconciler := &DatasetReconciler{Client: client, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(ctx, reconcileRequest(dataset)); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	securityReconciler := &SecurityProfileReconciler{Client: client, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
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
	if got := envVarValue(container.Env, "FUSEKI_WRITE_URL"); got != "https://standalone:3030" {
		t.Fatalf("unexpected write url: %q", got)
	}
	if got := envVarValue(container.Env, "SECURITY_PROFILE_TLS_CERT_FILE"); got != securityTLSCertFile {
		t.Fatalf("unexpected bootstrap TLS cert file: %q", got)
	}
	if mountPath := volumeMountPath(container.VolumeMounts, securityTLSVolumeName); mountPath != securityTLSMountPath {
		t.Fatalf("unexpected bootstrap TLS mount: %q", mountPath)
	}
	if got := envVarValue(container.Env, "SECURITY_PROFILE_OIDC_ISSUER"); got != "https://issuer.example.com" {
		t.Fatalf("unexpected bootstrap OIDC issuer: %q", got)
	}

	service := &corev1.Service{}
	if err := client.Get(ctx, objectKey(namespace.Name, server.ServiceName()), service); err != nil {
		t.Fatalf("get server service: %v", err)
	}
	if got := service.Spec.Selector["fuseki.apache.org/server"]; got != server.Name {
		t.Fatalf("unexpected server selector: %q", got)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(ctx, objectKey(namespace.Name, server.DeploymentName()), deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_OIDC_ISSUER"); got != "https://issuer.example.com" {
		t.Fatalf("unexpected runtime OIDC issuer: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "FUSEKI_SERVER_SCHEME"); got != "https" {
		t.Fatalf("unexpected runtime server scheme: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_SECRET"); got != "tls-secret" {
		t.Fatalf("unexpected runtime TLS secret: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_CERT_FILE"); got != securityTLSCertFile {
		t.Fatalf("unexpected runtime TLS cert file: %q", got)
	}
	if mountPath := volumeMountPath(deployment.Spec.Template.Spec.Containers[0].VolumeMounts, securityConfigVolumeName); mountPath != "/fuseki-extra/security" {
		t.Fatalf("unexpected security config mount: %q", mountPath)
	}
	if mountPath := volumeMountPath(deployment.Spec.Template.Spec.Containers[0].VolumeMounts, securityTLSVolumeName); mountPath != securityTLSMountPath {
		t.Fatalf("unexpected security TLS mount: %q", mountPath)
	}
	if deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet != nil {
		t.Fatalf("expected TLS readiness probe to use exec, got httpGet")
	}
	if deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec == nil || len(deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec.Command) != 3 {
		t.Fatalf("unexpected TLS readiness probe exec: %#v", deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec)
	}
	if got := deployment.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec.Command[2]; got != "curl --silent --show-error --fail --insecure https://127.0.0.1:3030/$/ping >/dev/null" {
		t.Fatalf("unexpected readiness probe command: %q", got)
	}

	securityConfig := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, profile.ConfigMapName()), securityConfig); err != nil {
		t.Fatalf("get security configmap: %v", err)
	}
	if !containsLine(securityConfig.Data["security.properties"], "oidc.issuerURL=https://issuer.example.com") || !containsLine(securityConfig.Data["security.properties"], "tls.certFile=/fuseki-extra/security/tls/tls.crt") {
		t.Fatalf("unexpected security properties: %q", securityConfig.Data["security.properties"])
	}
}

func TestEnvtestFusekiServerReconcilePreservesExistingBootstrapJobTemplate(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-server-existing-job"}}
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
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
			OIDCIssuerURL:             "https://issuer.example.com",
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: namespace.Name},
		StringData: map[string]string{"username": "admin", "password": "secret"},
	}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: namespace.Name}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: []byte("cert"), corev1.TLSPrivateKeyKey: []byte("key")}}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: namespace.Name},
		Spec:       fusekiv1alpha1.DatasetSpec{Name: "primary"},
	}

	for _, obj := range []ctrlclient.Object{server, profile, secret, tlsSecret, dataset} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	datasetReconciler := &DatasetReconciler{Client: client, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(ctx, reconcileRequest(dataset)); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	securityReconciler := &SecurityProfileReconciler{Client: client, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
	}

	target := datasetBootstrapTarget{
		Kind:               "server",
		Name:               server.Name,
		Image:              server.Spec.Image,
		ImagePullPolicy:    server.Spec.ImagePullPolicy,
		WriteURL:           "http://standalone:3030",
		SecurityProfileRef: server.Spec.SecurityProfileRef,
	}
	bootstrapJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      datasetBootstrapJobName(target, dataset.Name),
			Namespace: namespace.Name,
			Labels: mergeStringMaps(fusekiServerLabels(server), map[string]string{
				"fuseki.apache.org/component":    "dataset-bootstrap",
				"fuseki.apache.org/dataset":      dataset.Name,
				"fuseki.apache.org/dataset-name": dataset.Spec.Name,
			}),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptrTo(int32(3)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					"fuseki.apache.org/component":        "dataset-bootstrap",
					"fuseki.apache.org/dataset":          dataset.Name,
					"fuseki.apache.org/dataset-name":     dataset.Spec.Name,
					"fuseki.apache.org/server":           server.Name,
					"job-name":                           datasetBootstrapJobName(target, dataset.Name),
					"batch.kubernetes.io/controller-uid": "controller-uid",
					"batch.kubernetes.io/job-name":       datasetBootstrapJobName(target, dataset.Name),
					"controller-uid":                     "controller-uid",
				}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers:    []corev1.Container{datasetBootstrapContainer(dataset, target, profile, profile.Spec.AdminCredentialsSecretRef)},
					Volumes: []corev1.Volume{{
						Name:         datasetConfigVolumeName,
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: dataset.ConfigMapName()}}},
					}},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(server, bootstrapJob, scheme); err != nil {
		t.Fatalf("set controller reference: %v", err)
	}
	if err := client.Create(ctx, bootstrapJob); err != nil {
		t.Fatalf("create bootstrap job: %v", err)
	}

	serverReconciler := &FusekiServerReconciler{Client: client, Scheme: scheme}
	if _, err := serverReconciler.Reconcile(ctx, reconcileRequest(server)); err != nil {
		t.Fatalf("reconcile server with existing bootstrap job: %v", err)
	}

	updatedJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, bootstrapJob.Name), updatedJob); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := updatedJob.Spec.Template.Labels["batch.kubernetes.io/controller-uid"]; got != "controller-uid" {
		t.Fatalf("expected controller-added template labels to be preserved, got %q", got)
	}
	if got := updatedJob.Spec.Template.Labels["job-name"]; got != bootstrapJob.Name {
		t.Fatalf("unexpected job-name label: %q", got)
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

func TestEnvtestSecurityProfileStatusTransitions(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-security"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-auth", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
		},
	}
	if err := client.Create(ctx, profile); err != nil {
		t.Fatalf("create profile: %v", err)
	}

	reconciler := &SecurityProfileReconciler{Client: client, Scheme: scheme}
	if _, err := reconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile pending profile: %v", err)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := client.Get(ctx, objectKey(namespace.Name, profile.Name), updated); err != nil {
		t.Fatalf("get pending profile: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected pending phase: %q", updated.Status.Phase)
	}

	if err := client.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: namespace.Name}}); err != nil {
		t.Fatalf("create admin secret: %v", err)
	}
	if err := client.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: namespace.Name}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: []byte("cert"), corev1.TLSPrivateKeyKey: []byte("key")}}); err != nil {
		t.Fatalf("create tls secret: %v", err)
	}

	if _, err := reconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile ready profile: %v", err)
	}

	if err := client.Get(ctx, objectKey(namespace.Name, profile.Name), updated); err != nil {
		t.Fatalf("get ready profile: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected ready phase: %q", updated.Status.Phase)
	}
	if updated.Status.ConfigMapName != profile.ConfigMapName() {
		t.Fatalf("unexpected configmap status: %q", updated.Status.ConfigMapName)
	}
}

func TestEnvtestEndpointExposeFusekiServer(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-endpoint"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: namespace.Name},
		Spec:       fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0"},
	}
	endpoint := &fusekiv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.EndpointSpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiServer, Name: server.Name},
		},
	}

	if err := client.Create(ctx, server); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := client.Create(ctx, endpoint); err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	serverReconciler := &FusekiServerReconciler{Client: client, Scheme: scheme}
	if _, err := serverReconciler.Reconcile(ctx, reconcileRequest(server)); err != nil {
		t.Fatalf("reconcile server: %v", err)
	}

	endpointReconciler := &EndpointReconciler{Client: client, Scheme: scheme}
	if _, err := endpointReconciler.Reconcile(ctx, reconcileRequest(endpoint)); err != nil {
		t.Fatalf("reconcile endpoint: %v", err)
	}

	readService := &corev1.Service{}
	if err := client.Get(ctx, objectKey(namespace.Name, endpoint.ReadServiceName()), readService); err != nil {
		t.Fatalf("get read service: %v", err)
	}
	if got := readService.Spec.Selector["fuseki.apache.org/server"]; got != server.Name {
		t.Fatalf("unexpected endpoint selector: %q", got)
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
