package controller

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
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
			OIDC: &fusekiv1alpha1.SecurityOIDCSpec{
				IssuerURL: "https://issuer.example.com",
				ClientID:  "fuseki-ui",
			},
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
	if got := envVarValue(container.Env, "SECURITY_PROFILE_OIDC_CLIENT_ID"); got != "fuseki-ui" {
		t.Fatalf("unexpected bootstrap OIDC client ID: %q", got)
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
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_OIDC_CLIENT_ID"); got != "fuseki-ui" {
		t.Fatalf("unexpected runtime OIDC client ID: %q", got)
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
	if !containsLine(securityConfig.Data["security.properties"], "oidc.issuerURL=https://issuer.example.com") || !containsLine(securityConfig.Data["security.properties"], "oidc.clientID=fuseki-ui") || !containsLine(securityConfig.Data["security.properties"], "tls.certFile=/fuseki-extra/security/tls/tls.crt") {
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
			OIDC: &fusekiv1alpha1.SecurityOIDCSpec{
				IssuerURL: "https://issuer.example.com",
				ClientID:  "fuseki-ui",
			},
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
			ManualSelector: ptrTo(true),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"job-name":                           datasetBootstrapJobName(target, dataset.Name),
				"controller-uid":                     "controller-uid",
				"batch.kubernetes.io/job-name":       datasetBootstrapJobName(target, dataset.Name),
				"batch.kubernetes.io/controller-uid": "controller-uid",
			}},
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
					Volumes:       datasetBootstrapVolumes(dataset, profile),
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

func TestEnvtestFusekiUIIngressExposureFallsBackWhenGatewayUnavailable(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-fusekiui"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secured", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			TLSSecretRef: &corev1.LocalObjectReference{Name: "tls-secret"},
		},
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: namespace.Name},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       []byte("cert"),
			corev1.TLSPrivateKeyKey: []byte("key"),
		},
	}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			SecurityProfileRef: &corev1.LocalObjectReference{Name: profile.Name},
		},
	}
	ui := &fusekiv1alpha1.FusekiUI{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ui", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiUISpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiServer, Name: server.Name},
			Ingress: &fusekiv1alpha1.FusekiUIIngressSpec{
				Host:      "fuseki.example.test",
				ClassName: "nginx",
				TLSSecretRef: &corev1.LocalObjectReference{
					Name: "public-ui-cert",
				},
			},
			Gateway: &fusekiv1alpha1.FusekiUIGatewaySpec{
				ParentRefs: []fusekiv1alpha1.FusekiUIGatewayParentRef{{Name: "shared-gateway", Namespace: "infra", SectionName: "web"}},
				Hostnames:  []string{"fuseki.example.test"},
			},
		},
	}

	for _, obj := range []ctrlclient.Object{profile, tlsSecret, server, ui} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	securityReconciler := &SecurityProfileReconciler{Client: client, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
	}

	serverReconciler := &FusekiServerReconciler{Client: client, Scheme: scheme}
	if _, err := serverReconciler.Reconcile(ctx, reconcileRequest(server)); err != nil {
		t.Fatalf("reconcile server: %v", err)
	}

	uiReconciler := &FusekiUIReconciler{Client: client, Scheme: scheme}
	result, err := uiReconciler.Reconcile(ctx, reconcileRequest(ui))
	if err != nil {
		t.Fatalf("reconcile ui: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("expected %s requeue, got %s", securityProfileRequeueInterval, result.RequeueAfter)
	}

	service := &corev1.Service{}
	if err := client.Get(ctx, objectKey(namespace.Name, ui.ServiceName()), service); err != nil {
		t.Fatalf("get ui service: %v", err)
	}
	if got := service.Spec.Ports[0].Name; got != "https" {
		t.Fatalf("unexpected ui service port name: %q", got)
	}

	ingress := &networkingv1.Ingress{}
	if err := client.Get(ctx, objectKey(namespace.Name, ui.IngressName()), ingress); err != nil {
		t.Fatalf("get ui ingress: %v", err)
	}
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "nginx" {
		t.Fatalf("unexpected ingress class: %#v", ingress.Spec.IngressClassName)
	}
	if len(ingress.Spec.Rules) != 1 || ingress.Spec.Rules[0].Host != "fuseki.example.test" {
		t.Fatalf("unexpected ingress rules: %#v", ingress.Spec.Rules)
	}
	if len(ingress.Spec.TLS) != 1 || ingress.Spec.TLS[0].SecretName != "public-ui-cert" {
		t.Fatalf("unexpected ingress tls: %#v", ingress.Spec.TLS)
	}
	if ingress.Spec.Rules[0].HTTP == nil || len(ingress.Spec.Rules[0].HTTP.Paths) != 1 {
		t.Fatalf("unexpected ingress http paths: %#v", ingress.Spec.Rules[0].HTTP)
	}
	path := ingress.Spec.Rules[0].HTTP.Paths[0]
	if path.Backend.Service == nil || path.Backend.Service.Name != ui.ServiceName() || path.Backend.Service.Port.Number != server.DesiredHTTPPort() {
		t.Fatalf("unexpected ingress backend: %#v", path.Backend.Service)
	}

	updated := &fusekiv1alpha1.FusekiUI{}
	if err := client.Get(ctx, objectKey(namespace.Name, ui.Name), updated); err != nil {
		t.Fatalf("get updated ui: %v", err)
	}
	if updated.Status.IngressName != ui.IngressName() {
		t.Fatalf("unexpected ingress status name: %q", updated.Status.IngressName)
	}
	if updated.Status.HTTPRouteName != ui.HTTPRouteName() {
		t.Fatalf("unexpected route status name: %q", updated.Status.HTTPRouteName)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("expected Pending phase when Gateway API is unavailable, got %q", updated.Status.Phase)
	}
	ingressCondition := apimeta.FindStatusCondition(updated.Status.Conditions, ingressReadyConditionType)
	if ingressCondition == nil || ingressCondition.Status != metav1.ConditionTrue || ingressCondition.Reason != "IngressReady" {
		t.Fatalf("unexpected ingress condition: %#v", ingressCondition)
	}
	gatewayCondition := apimeta.FindStatusCondition(updated.Status.Conditions, gatewayReadyConditionType)
	if gatewayCondition == nil || gatewayCondition.Status != metav1.ConditionFalse || gatewayCondition.Reason != "GatewayAPIUnavailable" {
		t.Fatalf("unexpected gateway condition: %#v", gatewayCondition)
	}

	httpRoute := &unstructured.Unstructured{}
	httpRoute.SetGroupVersionKind(httpRouteGVK)
	err = client.Get(ctx, objectKey(namespace.Name, ui.HTTPRouteName()), httpRoute)
	if err == nil || !apimeta.IsNoMatchError(err) {
		t.Fatalf("expected HTTPRoute lookup to fail with no-match when Gateway API CRDs are absent, got %v", err)
	}
}

func TestEnvtestFusekiUIGatewayExposureCreatesHTTPRouteWhenCRDPresent(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClientWithAdditionalCRDPaths(t, filepath.Join("testdata", "gateway-api"))

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-fusekiui-gateway"}}
	if err := client.Create(ctx, namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secured", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			TLSSecretRef: &corev1.LocalObjectReference{Name: "tls-secret"},
		},
	}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: namespace.Name},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       []byte("cert"),
			corev1.TLSPrivateKeyKey: []byte("key"),
		},
	}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			SecurityProfileRef: &corev1.LocalObjectReference{Name: profile.Name},
		},
	}
	ui := &fusekiv1alpha1.FusekiUI{
		ObjectMeta: metav1.ObjectMeta{Name: "public-ui", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiUISpec{
			TargetRef: fusekiv1alpha1.EndpointTargetRef{Kind: fusekiv1alpha1.EndpointTargetKindFusekiServer, Name: server.Name},
			Gateway: &fusekiv1alpha1.FusekiUIGatewaySpec{
				ParentRefs: []fusekiv1alpha1.FusekiUIGatewayParentRef{{Name: "shared-gateway", Namespace: "infra", SectionName: "https"}},
				Hostnames:  []string{"fuseki.example.test"},
				Annotations: map[string]string{
					"gateway.networking.k8s.io/policy": "ui",
				},
			},
		},
	}

	for _, obj := range []ctrlclient.Object{profile, tlsSecret, server, ui} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	securityReconciler := &SecurityProfileReconciler{Client: client, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(ctx, reconcileRequest(profile)); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
	}

	serverReconciler := &FusekiServerReconciler{Client: client, Scheme: scheme}
	if _, err := serverReconciler.Reconcile(ctx, reconcileRequest(server)); err != nil {
		t.Fatalf("reconcile server: %v", err)
	}

	uiReconciler := &FusekiUIReconciler{Client: client, Scheme: scheme}
	result, err := uiReconciler.Reconcile(ctx, reconcileRequest(ui))
	if err != nil {
		t.Fatalf("reconcile ui: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue when HTTPRoute CRD is available, got %s", result.RequeueAfter)
	}

	service := &corev1.Service{}
	if err := client.Get(ctx, objectKey(namespace.Name, ui.ServiceName()), service); err != nil {
		t.Fatalf("get ui service: %v", err)
	}
	if got := service.Spec.Ports[0].Name; got != "https" {
		t.Fatalf("unexpected ui service port name: %q", got)
	}

	httpRoute := &unstructured.Unstructured{}
	httpRoute.SetGroupVersionKind(httpRouteGVK)
	if err := client.Get(ctx, objectKey(namespace.Name, ui.HTTPRouteName()), httpRoute); err != nil {
		t.Fatalf("get httproute: %v", err)
	}
	if got := httpRoute.GetAnnotations()["gateway.networking.k8s.io/policy"]; got != "ui" {
		t.Fatalf("unexpected route annotation: %q", got)
	}
	if got := httpRoute.GetAnnotations()["fuseki.apache.org/backend-scheme"]; got != "https" {
		t.Fatalf("unexpected route backend scheme annotation: %q", got)
	}
	parentRefs, found, err := unstructured.NestedSlice(httpRoute.Object, "spec", "parentRefs")
	if err != nil || !found || len(parentRefs) != 1 {
		t.Fatalf("unexpected httproute parentRefs: found=%t len=%d err=%v", found, len(parentRefs), err)
	}
	parentRef, ok := parentRefs[0].(map[string]any)
	if !ok || parentRef["name"] != "shared-gateway" || parentRef["namespace"] != "infra" || parentRef["sectionName"] != "https" {
		t.Fatalf("unexpected httproute parentRef: %#v", parentRefs[0])
	}
	hostnames, found, err := unstructured.NestedSlice(httpRoute.Object, "spec", "hostnames")
	if err != nil || !found || len(hostnames) != 1 || hostnames[0] != "fuseki.example.test" {
		t.Fatalf("unexpected httproute hostnames: found=%t values=%#v err=%v", found, hostnames, err)
	}
	rules, found, err := unstructured.NestedSlice(httpRoute.Object, "spec", "rules")
	if err != nil || !found || len(rules) != 1 {
		t.Fatalf("unexpected httproute rules: found=%t len=%d err=%v", found, len(rules), err)
	}
	rule, ok := rules[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected httproute rule payload: %T", rules[0])
	}
	backendRefs, ok := rule["backendRefs"].([]any)
	if !ok || len(backendRefs) != 1 {
		t.Fatalf("unexpected backendRefs payload: %#v", rule["backendRefs"])
	}
	backendRef, ok := backendRefs[0].(map[string]any)
	if !ok || backendRef["name"] != ui.ServiceName() || backendRef["port"] != int64(server.DesiredHTTPPort()) {
		t.Fatalf("unexpected backendRef payload: %#v", backendRefs[0])
	}

	updated := &fusekiv1alpha1.FusekiUI{}
	if err := client.Get(ctx, objectKey(namespace.Name, ui.Name), updated); err != nil {
		t.Fatalf("get updated ui: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("expected Ready phase when HTTPRoute CRD is available, got %q", updated.Status.Phase)
	}
	if updated.Status.HTTPRouteName != ui.HTTPRouteName() {
		t.Fatalf("unexpected route status name: %q", updated.Status.HTTPRouteName)
	}
	ingressCondition := apimeta.FindStatusCondition(updated.Status.Conditions, ingressReadyConditionType)
	if ingressCondition == nil || ingressCondition.Status != metav1.ConditionTrue || ingressCondition.Reason != "IngressNotConfigured" {
		t.Fatalf("unexpected ingress condition: %#v", ingressCondition)
	}
	gatewayCondition := apimeta.FindStatusCondition(updated.Status.Conditions, gatewayReadyConditionType)
	if gatewayCondition == nil || gatewayCondition.Status != metav1.ConditionTrue || gatewayCondition.Reason != "GatewayReady" {
		t.Fatalf("unexpected gateway condition: %#v", gatewayCondition)
	}
}

func TestEnvtestChangeSubscriptionResumesFromStoredCheckpointAfterRestart(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	previousFetcher := rdfDeltaLogVersionFetcher
	currentVersion := 0
	rdfDeltaLogVersionFetcher = func(context.Context, string, string) (int, error) {
		return currentVersion, nil
	}
	t.Cleanup(func() { rdfDeltaLogVersionFetcher = previousFetcher })

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-subscription-resume"}}
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: namespace.Name},
		Spec:       fusekiv1alpha1.RDFDeltaServerSpec{Image: "ghcr.io/example/rdf-delta:latest"},
	}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/"},
		},
		Status: fusekiv1alpha1.ChangeSubscriptionStatus{LastCheckpoint: "5"},
	}
	for _, obj := range []ctrlclient.Object{namespace, server, dataset, subscription} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	storedSubscription := persistChangeSubscriptionStatus(t, ctx, client, namespace.Name, subscription.Name, "5")
	completedStartTime := metav1.Now()
	completedCompletionTime := metav1.NewTime(completedStartTime.Time.Add(time.Minute))
	completedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      changeSubscriptionJobName(subscription),
			Namespace: namespace.Name,
			Annotations: map[string]string{
				subscriptionGenerationAnnotation:      strconv.FormatInt(storedSubscription.Generation, 10),
				subscriptionStartCheckpointAnnotation: "6",
				subscriptionEndCheckpointAnnotation:   "7",
				subscriptionArtifactRefAnnotation:     "/exports/example-subscription-000000000006-000000000007.rdfpatch",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "delivery", Image: "ghcr.io/example/rdf-delta:latest"}},
				},
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		},
	}
	if err := client.Create(ctx, completedJob); err != nil {
		t.Fatalf("create %T: %v", completedJob, err)
	}
	persistJobStatus(t, ctx, client, namespace.Name, completedJob.Name, batchv1.JobStatus{
		StartTime:      &completedStartTime,
		CompletionTime: &completedCompletionTime,
		Succeeded:      1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		},
	})

	firstReconciler := &ChangeSubscriptionReconciler{Client: client, Scheme: scheme}
	result, err := firstReconciler.Reconcile(ctx, reconcileRequest(subscription))
	if err != nil {
		t.Fatalf("first reconcile subscription: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after checkpoint advance, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := client.Get(ctx, objectKey(namespace.Name, subscription.Name), updated); err != nil {
		t.Fatalf("get updated subscription after checkpoint advance: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected phase after checkpoint advance: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "7" {
		t.Fatalf("unexpected checkpoint after checkpoint advance: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionDelivered" {
		t.Fatalf("expected delivered condition after checkpoint advance, got %#v", deliveryCondition)
	}

	if err := waitForJobDeletion(ctx, client, namespace.Name, completedJob.Name); err != nil {
		t.Fatalf("expected completed delivery job to be deleted: %v", err)
	}

	currentVersion = 9
	secondReconciler := &ChangeSubscriptionReconciler{Client: client, Scheme: scheme}
	result, err = secondReconciler.Reconcile(ctx, reconcileRequest(subscription))
	if err != nil {
		t.Fatalf("second reconcile subscription: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after resumed delivery, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	if err := client.Get(ctx, objectKey(namespace.Name, subscription.Name), updated); err != nil {
		t.Fatalf("get updated subscription after resumed delivery: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase after resumed delivery: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "7" {
		t.Fatalf("unexpected checkpoint after resumed delivery: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition = apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionLagging" {
		t.Fatalf("expected lagging condition after resumed delivery, got %#v", deliveryCondition)
	}

	resumedJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, changeSubscriptionJobName(subscription)), resumedJob); err != nil {
		t.Fatalf("get resumed delivery job: %v", err)
	}
	container := resumedJob.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "SUBSCRIPTION_START_VERSION"); got != "8" {
		t.Fatalf("unexpected resumed start version: %q", got)
	}
	if got := envVarValue(container.Env, "SUBSCRIPTION_END_VERSION"); got != "9" {
		t.Fatalf("unexpected resumed end version: %q", got)
	}
	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, changeSubscriptionSummaryConfigMapName(subscription)), summary); err != nil {
		t.Fatalf("get resumed subscription summary: %v", err)
	}
	if got := summary.Data["lastCheckpoint"]; got != "7" {
		t.Fatalf("unexpected summary checkpoint after resumed delivery: %q", got)
	}
	if got := summary.Data["pendingRange"]; got != "8-9" {
		t.Fatalf("unexpected summary pending range after resumed delivery: %q", got)
	}
	if got := summary.Data["lag"]; got != "2" {
		t.Fatalf("unexpected summary lag after resumed delivery: %q", got)
	}
}

func TestEnvtestChangeSubscriptionRetriesFailedDeliveryAfterRestart(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	previousFetcher := rdfDeltaLogVersionFetcher
	rdfDeltaLogVersionFetcher = func(context.Context, string, string) (int, error) {
		return 7, nil
	}
	t.Cleanup(func() { rdfDeltaLogVersionFetcher = previousFetcher })

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-subscription-retry"}}
	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: namespace.Name},
		Spec:       fusekiv1alpha1.RDFDeltaServerSpec{Image: "ghcr.io/example/rdf-delta:latest"},
	}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/"},
		},
		Status: fusekiv1alpha1.ChangeSubscriptionStatus{LastCheckpoint: "5"},
	}
	for _, obj := range []ctrlclient.Object{namespace, server, dataset, subscription} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	storedSubscription := persistChangeSubscriptionStatus(t, ctx, client, namespace.Name, subscription.Name, "5")
	failedStartTime := metav1.Now()
	failedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      changeSubscriptionJobName(subscription),
			Namespace: namespace.Name,
			Annotations: map[string]string{
				subscriptionGenerationAnnotation:      strconv.FormatInt(storedSubscription.Generation, 10),
				subscriptionStartCheckpointAnnotation: "6",
				subscriptionEndCheckpointAnnotation:   "7",
				subscriptionArtifactRefAnnotation:     "/exports/example-subscription-000000000006-000000000007.rdfpatch",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "delivery", Image: "ghcr.io/example/rdf-delta:latest"}},
				},
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		},
	}
	if err := client.Create(ctx, failedJob); err != nil {
		t.Fatalf("create %T: %v", failedJob, err)
	}
	persistJobStatus(t, ctx, client, namespace.Name, failedJob.Name, batchv1.JobStatus{
		StartTime: &failedStartTime,
		Failed:    1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		},
	})

	firstReconciler := &ChangeSubscriptionReconciler{Client: client, Scheme: scheme}
	result, err := firstReconciler.Reconcile(ctx, reconcileRequest(subscription))
	if err != nil {
		t.Fatalf("first reconcile subscription: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after failed delivery, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := client.Get(ctx, objectKey(namespace.Name, subscription.Name), updated); err != nil {
		t.Fatalf("get updated subscription after failed delivery: %v", err)
	}
	if updated.Status.Phase != "Failed" {
		t.Fatalf("unexpected phase after failed delivery: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "5" {
		t.Fatalf("unexpected checkpoint after failed delivery: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionFailed" {
		t.Fatalf("expected failed condition after failed delivery, got %#v", deliveryCondition)
	}

	if err := waitForJobDeletion(ctx, client, namespace.Name, failedJob.Name); err != nil {
		t.Fatalf("expected failed delivery job to be deleted: %v", err)
	}

	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, changeSubscriptionSummaryConfigMapName(subscription)), summary); err != nil {
		t.Fatalf("get failed delivery summary: %v", err)
	}
	if got := summary.Data["deliveryReason"]; got != "SubscriptionFailed" {
		t.Fatalf("unexpected delivery reason after failed delivery: %q", got)
	}
	if got := summary.Data["pendingRange"]; got != "6-7" {
		t.Fatalf("unexpected pending range after failed delivery: %q", got)
	}

	secondReconciler := &ChangeSubscriptionReconciler{Client: client, Scheme: scheme}
	result, err = secondReconciler.Reconcile(ctx, reconcileRequest(subscription))
	if err != nil {
		t.Fatalf("second reconcile subscription: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after retry delivery, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	if err := client.Get(ctx, objectKey(namespace.Name, subscription.Name), updated); err != nil {
		t.Fatalf("get updated subscription after retry delivery: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase after retry delivery: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "5" {
		t.Fatalf("unexpected checkpoint after retry delivery: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition = apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionLagging" {
		t.Fatalf("expected lagging condition after retry delivery, got %#v", deliveryCondition)
	}

	retriedJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, changeSubscriptionJobName(subscription)), retriedJob); err != nil {
		t.Fatalf("get retried delivery job: %v", err)
	}
	container := retriedJob.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "SUBSCRIPTION_START_VERSION"); got != "6" {
		t.Fatalf("unexpected retried start version: %q", got)
	}
	if got := envVarValue(container.Env, "SUBSCRIPTION_END_VERSION"); got != "7" {
		t.Fatalf("unexpected retried end version: %q", got)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, changeSubscriptionSummaryConfigMapName(subscription)), summary); err != nil {
		t.Fatalf("get retried delivery summary: %v", err)
	}
	if got := summary.Data["deliveryReason"]; got != "SubscriptionLagging" {
		t.Fatalf("unexpected delivery reason after retry delivery: %q", got)
	}
	if got := summary.Data["pendingRange"]; got != "6-7" {
		t.Fatalf("unexpected pending range after retry delivery: %q", got)
	}
	if got := summary.Data["lag"]; got != "2" {
		t.Fatalf("unexpected lag after retry delivery: %q", got)
	}
}

func TestEnvtestIngestPipelineRetainsCompletedStatusAfterRestart(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-ingest-complete"}}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:       "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}},
		},
	}
	policy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	for _, obj := range []ctrlclient.Object{namespace, dataset, server, policy, pipeline} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	storedPolicy := persistSHACLPolicyConfigured(t, ctx, client, namespace.Name, policy.Name)
	storedPipeline := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), storedPipeline); err != nil {
		t.Fatalf("get pipeline before job creation: %v", err)
	}
	completedStartTime := metav1.Now()
	completedCompletionTime := metav1.NewTime(completedStartTime.Time.Add(time.Minute))
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingestPipelineJobName(storedPipeline),
			Namespace: namespace.Name,
			Annotations: map[string]string{
				ingestGenerationAnnotation:      strconv.FormatInt(storedPipeline.Generation, 10),
				ingestReportDirectoryAnnotation: ingestReportDirectory,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "ingest", Image: server.Spec.Image}},
				},
			},
		},
	}
	if err := client.Create(ctx, job); err != nil {
		t.Fatalf("create ingest job: %v", err)
	}
	persistJobStatus(t, ctx, client, namespace.Name, job.Name, batchv1.JobStatus{
		StartTime:      &completedStartTime,
		CompletionTime: &completedCompletionTime,
		Succeeded:      1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		},
	})

	reconciler := &IngestPipelineReconciler{Client: client, Scheme: scheme}
	result, err := reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile completed ingest pipeline: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for completed one-shot ingest, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), updated); err != nil {
		t.Fatalf("get updated ingest pipeline: %v", err)
	}
	if updated.Status.Phase != "Succeeded" {
		t.Fatalf("unexpected phase after completed ingest: %q", updated.Status.Phase)
	}
	if updated.Status.LastRunTime == nil {
		t.Fatalf("unexpected last run time after completed ingest: %#v", updated.Status.LastRunTime)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if condition == nil || condition.Reason != "IngestCompleted" {
		t.Fatalf("expected IngestCompleted condition, got %#v", condition)
	}
	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineSummaryConfigMapName(updated)), summary); err != nil {
		t.Fatalf("get ingest summary: %v", err)
	}
	if got := summary.Data["phase"]; got != "Succeeded" {
		t.Fatalf("unexpected summary phase after completed ingest: %q", got)
	}
	if got := summary.Data["executionReason"]; got != "IngestCompleted" {
		t.Fatalf("unexpected summary execution reason after completed ingest: %q", got)
	}
	if got := summary.Data["lastRunTime"]; got == "" {
		t.Fatalf("expected summary lastRunTime after completed ingest, got empty value")
	}
	if got := summary.Data["failureAction"]; got != string(storedPolicy.DesiredFailureAction()) {
		t.Fatalf("unexpected summary failure action after completed ingest: %q", got)
	}

	persistedJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, job.Name), persistedJob); err != nil {
		t.Fatalf("get completed ingest job before restart reconcile: %v", err)
	}
	jobResourceVersion := persistedJob.ResourceVersion

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile completed ingest pipeline after restart: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue when completed ingest status is already persisted, got %s", result.RequeueAfter)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, job.Name), persistedJob); err != nil {
		t.Fatalf("get completed ingest job after restart reconcile: %v", err)
	}
	if persistedJob.ResourceVersion != jobResourceVersion {
		t.Fatalf("expected completed ingest job to remain unchanged across restart reconcile, got resourceVersion %q -> %q", jobResourceVersion, persistedJob.ResourceVersion)
	}
}

func TestEnvtestIngestPipelineRetriesFailedJobAfterGenerationChange(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-ingest-retry"}}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:       "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}},
		},
	}
	policy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
			FailureAction: fusekiv1alpha1.SHACLFailureActionReportOnly,
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	for _, obj := range []ctrlclient.Object{namespace, dataset, server, policy, pipeline} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	storedPolicy := persistSHACLPolicyConfigured(t, ctx, client, namespace.Name, policy.Name)
	storedPipeline := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), storedPipeline); err != nil {
		t.Fatalf("get pipeline before failed job creation: %v", err)
	}
	failedStartTime := metav1.Now()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingestPipelineJobName(storedPipeline),
			Namespace: namespace.Name,
			Annotations: map[string]string{
				ingestGenerationAnnotation:      strconv.FormatInt(storedPipeline.Generation, 10),
				ingestReportDirectoryAnnotation: ingestReportDirectory,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "ingest", Image: server.Spec.Image}},
				},
			},
		},
	}
	if err := client.Create(ctx, job); err != nil {
		t.Fatalf("create failed ingest job: %v", err)
	}
	persistJobStatus(t, ctx, client, namespace.Name, job.Name, batchv1.JobStatus{
		StartTime: &failedStartTime,
		Failed:    1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		},
	})

	reconciler := &IngestPipelineReconciler{Client: client, Scheme: scheme}
	result, err := reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile failed ingest pipeline: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for failed one-shot ingest, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), updated); err != nil {
		t.Fatalf("get failed ingest pipeline: %v", err)
	}
	if updated.Status.Phase != "Failed" {
		t.Fatalf("unexpected phase after failed ingest: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if condition == nil || condition.Reason != "IngestFailed" {
		t.Fatalf("expected IngestFailed condition, got %#v", condition)
	}
	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineSummaryConfigMapName(updated)), summary); err != nil {
		t.Fatalf("get failed ingest summary: %v", err)
	}
	if got := summary.Data["executionReason"]; got != "IngestFailed" {
		t.Fatalf("unexpected summary execution reason after failed ingest: %q", got)
	}
	if got := summary.Data["failureAction"]; got != string(storedPolicy.DesiredFailureAction()) {
		t.Fatalf("unexpected summary failure action after failed ingest: %q", got)
	}

	retryPipeline := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), retryPipeline); err != nil {
		t.Fatalf("get ingest pipeline before retry update: %v", err)
	}
	retryPipeline.Spec.Source.URI = "https://example.com/retry.ttl"
	if err := client.Update(ctx, retryPipeline); err != nil {
		t.Fatalf("update ingest pipeline for retry: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), retryPipeline); err != nil {
		t.Fatalf("get ingest pipeline after retry update: %v", err)
	}

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile ingest retry cleanup: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s while replacing failed ingest job, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}
	if err := waitForJobDeletion(ctx, client, namespace.Name, job.Name); err != nil {
		t.Fatalf("expected failed ingest job to be deleted before retry: %v", err)
	}

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile ingest retry creation: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after creating retry ingest job, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), updated); err != nil {
		t.Fatalf("get ingest pipeline after retry creation: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase after retry ingest job creation: %q", updated.Status.Phase)
	}
	condition = apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if condition == nil || condition.Reason != "IngestPending" {
		t.Fatalf("expected IngestPending condition after retry creation, got %#v", condition)
	}
	retriedJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineJobName(updated)), retriedJob); err != nil {
		t.Fatalf("get retried ingest job: %v", err)
	}
	if got := retriedJob.Annotations[ingestGenerationAnnotation]; got != strconv.FormatInt(retryPipeline.Generation, 10) {
		t.Fatalf("unexpected retried ingest job generation annotation: %q", got)
	}
	container := retriedJob.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "TRANSFER_SOURCE_URI"); got != "https://example.com/retry.ttl" {
		t.Fatalf("unexpected retried ingest source URI: %q", got)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineSummaryConfigMapName(updated)), summary); err != nil {
		t.Fatalf("get retried ingest summary: %v", err)
	}
	if got := summary.Data["executionReason"]; got != "IngestPending" {
		t.Fatalf("unexpected summary execution reason after retry creation: %q", got)
	}
	if got := summary.Data["failureAction"]; got != string(storedPolicy.DesiredFailureAction()) {
		t.Fatalf("unexpected summary failure action after retry creation: %q", got)
	}
	if got := summary.Data["targetKind"]; got != "Job" {
		t.Fatalf("unexpected summary target kind after retry creation: %q", got)
	}
}

func TestEnvtestIngestPipelineScheduledCronJobPersistsAcrossRestart(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-ingest-scheduled"}}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:       "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}},
		},
	}
	policy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
			Schedule:       "*/15 * * * *",
		},
	}
	for _, obj := range []ctrlclient.Object{namespace, dataset, server, policy, pipeline} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	persistSHACLPolicyConfigured(t, ctx, client, namespace.Name, policy.Name)
	reconciler := &IngestPipelineReconciler{Client: client, Scheme: scheme}
	result, err := reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile scheduled ingest pipeline: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s for scheduled ingest, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	cronJob := &batchv1.CronJob{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineCronJobName(pipeline)), cronJob); err != nil {
		t.Fatalf("get ingest cronjob: %v", err)
	}
	lastScheduleTime := metav1.Now()
	persistCronJobStatus(t, ctx, client, namespace.Name, cronJob.Name, batchv1.CronJobStatus{LastScheduleTime: &lastScheduleTime})

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile scheduled ingest pipeline after status: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after scheduled status, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), updated); err != nil {
		t.Fatalf("get updated scheduled ingest pipeline: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase after scheduled ingest reconcile: %q", updated.Status.Phase)
	}
	if updated.Status.LastRunTime == nil {
		t.Fatalf("expected scheduled ingest last run time to be recorded")
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if condition == nil || condition.Reason != "IngestScheduled" {
		t.Fatalf("expected IngestScheduled condition, got %#v", condition)
	}
	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineSummaryConfigMapName(updated)), summary); err != nil {
		t.Fatalf("get scheduled ingest summary: %v", err)
	}
	if got := summary.Data["targetKind"]; got != "CronJob" {
		t.Fatalf("unexpected summary target kind for scheduled ingest: %q", got)
	}
	if got := summary.Data["executionReason"]; got != "IngestScheduled" {
		t.Fatalf("unexpected summary execution reason for scheduled ingest: %q", got)
	}
	if got := summary.Data["schedule"]; got != pipeline.Spec.Schedule {
		t.Fatalf("unexpected summary schedule for scheduled ingest: %q", got)
	}

	persistedCronJob := &batchv1.CronJob{}
	if err := client.Get(ctx, objectKey(namespace.Name, cronJob.Name), persistedCronJob); err != nil {
		t.Fatalf("get scheduled cronjob before restart reconcile: %v", err)
	}
	cronJobResourceVersion := persistedCronJob.ResourceVersion

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile scheduled ingest pipeline after restart: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s after scheduled restart reconcile, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, cronJob.Name), persistedCronJob); err != nil {
		t.Fatalf("get scheduled cronjob after restart reconcile: %v", err)
	}
	if persistedCronJob.ResourceVersion != cronJobResourceVersion {
		t.Fatalf("expected scheduled cronjob to remain unchanged across restart reconcile, got resourceVersion %q -> %q", cronJobResourceVersion, persistedCronJob.ResourceVersion)
	}
	staleJob := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineJobName(pipeline)), staleJob); !apierrors.IsNotFound(err) {
		t.Fatalf("expected no one-shot ingest job for scheduled pipeline, got %v", err)
	}
}

func TestEnvtestIngestPipelineTransitionsFromFailedJobToScheduledCronJob(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	_, client, scheme := startEnvtestClient(t)

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "envtest-ingest-scheduled-transition"}}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: namespace.Name}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:       "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}},
		},
	}
	policy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
			FailureAction: fusekiv1alpha1.SHACLFailureActionReportOnly,
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: namespace.Name},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: policy.Name},
		},
	}
	for _, obj := range []ctrlclient.Object{namespace, dataset, server, policy, pipeline} {
		if err := client.Create(ctx, obj); err != nil {
			t.Fatalf("create %T: %v", obj, err)
		}
	}

	storedPolicy := persistSHACLPolicyConfigured(t, ctx, client, namespace.Name, policy.Name)
	storedPipeline := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), storedPipeline); err != nil {
		t.Fatalf("get pipeline before failed ingest job creation: %v", err)
	}
	failedStartTime := metav1.Now()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingestPipelineJobName(storedPipeline),
			Namespace: namespace.Name,
			Annotations: map[string]string{
				ingestGenerationAnnotation:      strconv.FormatInt(storedPipeline.Generation, 10),
				ingestReportDirectoryAnnotation: ingestReportDirectory,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "ingest", Image: server.Spec.Image}},
				},
			},
		},
	}
	if err := client.Create(ctx, job); err != nil {
		t.Fatalf("create failed ingest job: %v", err)
	}
	persistJobStatus(t, ctx, client, namespace.Name, job.Name, batchv1.JobStatus{
		StartTime: &failedStartTime,
		Failed:    1,
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		},
	})

	reconciler := &IngestPipelineReconciler{Client: client, Scheme: scheme}
	result, err := reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile failed one-shot ingest before scheduled transition: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for failed one-shot ingest, got %s", result.RequeueAfter)
	}

	retryPipeline := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), retryPipeline); err != nil {
		t.Fatalf("get ingest pipeline before schedule update: %v", err)
	}
	retryPipeline.Spec.Schedule = "0 * * * *"
	retryPipeline.Spec.Source.URI = "https://example.com/scheduled-retry.ttl"
	if err := client.Update(ctx, retryPipeline); err != nil {
		t.Fatalf("update ingest pipeline to scheduled mode: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), retryPipeline); err != nil {
		t.Fatalf("get ingest pipeline after schedule update: %v", err)
	}

	result, err = reconciler.Reconcile(ctx, reconcileRequest(pipeline))
	if err != nil {
		t.Fatalf("reconcile scheduled ingest transition: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s for scheduled transition, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}
	if err := waitForJobDeletion(ctx, client, namespace.Name, job.Name); err != nil {
		t.Fatalf("expected failed one-shot ingest job to be deleted after scheduled transition: %v", err)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := client.Get(ctx, objectKey(namespace.Name, pipeline.Name), updated); err != nil {
		t.Fatalf("get ingest pipeline after scheduled transition: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase after scheduled transition: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if condition == nil || condition.Reason != "IngestScheduled" {
		t.Fatalf("expected IngestScheduled condition after scheduled transition, got %#v", condition)
	}
	cronJob := &batchv1.CronJob{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineCronJobName(updated)), cronJob); err != nil {
		t.Fatalf("get scheduled ingest cronjob after transition: %v", err)
	}
	if cronJob.Spec.Schedule != retryPipeline.Spec.Schedule {
		t.Fatalf("unexpected cronjob schedule after transition: %q", cronJob.Spec.Schedule)
	}
	container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "TRANSFER_SOURCE_URI"); got != "https://example.com/scheduled-retry.ttl" {
		t.Fatalf("unexpected scheduled ingest source URI after transition: %q", got)
	}
	if got := cronJob.Annotations[ingestGenerationAnnotation]; got != strconv.FormatInt(retryPipeline.Generation, 10) {
		t.Fatalf("unexpected cronjob generation annotation after transition: %q", got)
	}
	summary := &corev1.ConfigMap{}
	if err := client.Get(ctx, objectKey(namespace.Name, ingestPipelineSummaryConfigMapName(updated)), summary); err != nil {
		t.Fatalf("get scheduled transition summary: %v", err)
	}
	if got := summary.Data["targetKind"]; got != "CronJob" {
		t.Fatalf("unexpected summary target kind after scheduled transition: %q", got)
	}
	if got := summary.Data["executionReason"]; got != "IngestScheduled" {
		t.Fatalf("unexpected summary execution reason after scheduled transition: %q", got)
	}
	if got := summary.Data["failureAction"]; got != string(storedPolicy.DesiredFailureAction()) {
		t.Fatalf("unexpected summary failure action after scheduled transition: %q", got)
	}
}

func startEnvtestClient(t *testing.T) (*envtest.Environment, ctrlclient.Client, *runtime.Scheme) {
	t.Helper()
	return startEnvtestClientWithAdditionalCRDPaths(t)
}

func persistSHACLPolicyConfigured(t *testing.T, ctx context.Context, client ctrlclient.Client, namespace, name string) *fusekiv1alpha1.SHACLPolicy {
	t.Helper()

	policy := &fusekiv1alpha1.SHACLPolicy{}
	if err := client.Get(ctx, objectKey(namespace, name), policy); err != nil {
		t.Fatalf("get SHACL policy before status update: %v", err)
	}
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.Phase = "Ready"
	policy.Status.Conditions = []metav1.Condition{{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "SourcesResolved",
		Message:            "SHACL sources are resolved.",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: policy.Generation,
	}}
	if err := client.Status().Update(ctx, policy); err != nil {
		t.Fatalf("update SHACL policy status: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace, name), policy); err != nil {
		t.Fatalf("get SHACL policy after status update: %v", err)
	}
	return policy
}

func persistChangeSubscriptionStatus(t *testing.T, ctx context.Context, client ctrlclient.Client, namespace, name, checkpoint string) *fusekiv1alpha1.ChangeSubscription {
	t.Helper()

	subscription := &fusekiv1alpha1.ChangeSubscription{}
	if err := client.Get(ctx, objectKey(namespace, name), subscription); err != nil {
		t.Fatalf("get subscription before status update: %v", err)
	}
	subscription.Status.LastCheckpoint = checkpoint
	if err := client.Status().Update(ctx, subscription); err != nil {
		t.Fatalf("update subscription status: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace, name), subscription); err != nil {
		t.Fatalf("get subscription after status update: %v", err)
	}
	return subscription
}

func persistJobStatus(t *testing.T, ctx context.Context, client ctrlclient.Client, namespace, name string, status batchv1.JobStatus) *batchv1.Job {
	t.Helper()

	job := &batchv1.Job{}
	if err := client.Get(ctx, objectKey(namespace, name), job); err != nil {
		t.Fatalf("get job before status update: %v", err)
	}
	job.Status = status
	if err := client.Status().Update(ctx, job); err != nil {
		t.Fatalf("update job status: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace, name), job); err != nil {
		t.Fatalf("get job after status update: %v", err)
	}
	return job
}

func persistCronJobStatus(t *testing.T, ctx context.Context, client ctrlclient.Client, namespace, name string, status batchv1.CronJobStatus) *batchv1.CronJob {
	t.Helper()

	cronJob := &batchv1.CronJob{}
	if err := client.Get(ctx, objectKey(namespace, name), cronJob); err != nil {
		t.Fatalf("get cronjob before status update: %v", err)
	}
	cronJob.Status = status
	if err := client.Status().Update(ctx, cronJob); err != nil {
		t.Fatalf("update cronjob status: %v", err)
	}
	if err := client.Get(ctx, objectKey(namespace, name), cronJob); err != nil {
		t.Fatalf("get cronjob after status update: %v", err)
	}
	return cronJob
}

func waitForJobDeletion(ctx context.Context, client ctrlclient.Client, namespace, name string) error {
	for range 50 {
		job := &batchv1.Job{}
		err := client.Get(ctx, objectKey(namespace, name), job)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if job.DeletionTimestamp != nil && len(job.Finalizers) > 0 {
			job.Finalizers = nil
			if err := client.Update(ctx, job); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func startEnvtestClientWithAdditionalCRDPaths(t *testing.T, additionalCRDPaths ...string) (*envtest.Environment, ctrlclient.Client, *runtime.Scheme) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "bases")
	if _, err := os.Stat(crdPath); err != nil {
		t.Fatalf("stat CRD directory: %v", err)
	}
	crdPaths := []string{crdPath}
	for _, extraPath := range additionalCRDPaths {
		if _, err := os.Stat(extraPath); err != nil {
			t.Fatalf("stat additional CRD directory %q: %v", extraPath, err)
		}
		crdPaths = append(crdPaths, extraPath)
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

	env := &envtest.Environment{CRDDirectoryPaths: crdPaths, ErrorIfCRDPathMissing: true, BinaryAssetsDirectory: binaryDir}
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

func datasetBootstrapVolumes(dataset *fusekiv1alpha1.Dataset, securityProfile *fusekiv1alpha1.SecurityProfile) []corev1.Volume {
	volumes := []corev1.Volume{{
		Name:         datasetConfigVolumeName,
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: dataset.ConfigMapName()}}},
	}}
	if securityVolume := fusekiSecurityConfigVolume(securityProfile); securityVolume != nil {
		volumes = append(volumes, *securityVolume)
	}
	if tlsVolume := fusekiSecurityTLSVolume(securityProfile); tlsVolume != nil {
		volumes = append(volumes, *tlsVolume)
	}
	return volumes
}

func readyPod(namespace, clusterName, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   clusterName,
				"fuseki.apache.org/component": "server",
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
