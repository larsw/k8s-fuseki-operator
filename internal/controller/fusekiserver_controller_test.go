package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
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
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
			OIDCIssuerURL:             "https://dex.example.com/dex",
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: "default"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: []byte("cert"), corev1.TLSPrivateKeyKey: []byte("key")}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiServer{}).
		WithObjects(server, dataset, profile, secret, tlsSecret).
		Build()

	securityReconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)}); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
	}

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
	if got := configMap.Data["run-fuseki.sh"]; !containsLine(got, "  exec \"${JAVA_HOME}/bin/java\" -cp /opt/fuseki/fuseki-server.jar:/opt/fuseki/fuseki-operator-launcher.jar FusekiHttpsLauncher --https=\"${https_config}\" --httpsPort ${FUSEKI_PORT:-3030}") {
		t.Fatalf("unexpected TLS startup script: %q", got)
	}
	if got := configMap.Data["run-fuseki.sh"]; !containsLine(got, "  rm -f \"${FUSEKI_BASE}/shiro.ini\"") {
		t.Fatalf("expected TLS startup script to disable shiro: %q", got)
	}

	service := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone"}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if got := service.Spec.Ports[0].Port; got != 3030 {
		t.Fatalf("unexpected service port: %d", got)
	}
	if got := service.Spec.Ports[0].Name; got != "https" {
		t.Fatalf("unexpected service port name: %q", got)
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
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_OIDC_ISSUER"); got != "https://dex.example.com/dex" {
		t.Fatalf("unexpected OIDC issuer env var: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "FUSEKI_SERVER_SCHEME"); got != "https" {
		t.Fatalf("unexpected server scheme env var: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_SECRET"); got != "tls-secret" {
		t.Fatalf("unexpected TLS secret env var: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_CERT_FILE"); got != securityTLSCertFile {
		t.Fatalf("unexpected TLS cert env var: %q", got)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_KEY_FILE"); got != securityTLSKeyFile {
		t.Fatalf("unexpected TLS key env var: %q", got)
	}
	if secretName := envVarSecretRefName(deployment.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); secretName != "admin-secret" {
		t.Fatalf("unexpected deployment admin password secret: %q", secretName)
	}
	if mountPath := volumeMountPath(deployment.Spec.Template.Spec.Containers[0].VolumeMounts, securityConfigVolumeName); mountPath != "/fuseki-extra/security" {
		t.Fatalf("unexpected security config mount: %q", mountPath)
	}
	if mountPath := volumeMountPath(deployment.Spec.Template.Spec.Containers[0].VolumeMounts, securityTLSVolumeName); mountPath != securityTLSMountPath {
		t.Fatalf("unexpected security TLS mount: %q", mountPath)
	}
	if configMapName := configMapVolumeName(deployment.Spec.Template.Spec.Volumes, securityConfigVolumeName); configMapName != "admin-auth-security" {
		t.Fatalf("unexpected security config volume source: %q", configMapName)
	}
	if secretName := secretVolumeName(deployment.Spec.Template.Spec.Volumes, securityTLSVolumeName); secretName != "tls-secret" {
		t.Fatalf("unexpected security TLS volume source: %q", secretName)
	}
	for probeName, probe := range map[string]*corev1.Probe{
		"startup":   deployment.Spec.Template.Spec.Containers[0].StartupProbe,
		"readiness": deployment.Spec.Template.Spec.Containers[0].ReadinessProbe,
		"liveness":  deployment.Spec.Template.Spec.Containers[0].LivenessProbe,
	} {
		if probe.HTTPGet != nil {
			t.Fatalf("expected %s probe to use exec for TLS, got httpGet", probeName)
		}
		if probe.Exec == nil || len(probe.Exec.Command) != 3 {
			t.Fatalf("expected %s probe exec command, got %#v", probeName, probe.Exec)
		}
		if got := probe.Exec.Command[2]; got != "curl --silent --show-error --fail --insecure https://127.0.0.1:3030/$/ping >/dev/null" {
			t.Fatalf("unexpected %s probe command: %q", probeName, got)
		}
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "server-standalone-dataset-a-bootstrap"}, job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_WRITE_URL"); got != "https://standalone:3030" {
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

func TestFusekiServerReconcileDefersBootstrapUntilSecurityReady(t *testing.T) {
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

	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "dataset-a", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "dataset-a"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "missing-profile"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiServer{}).
		WithObjects(server, dataset).
		Build()

	datasetReconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	reconciler := &FusekiServerReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)})
	if err != nil {
		t.Fatalf("reconcile server: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("unexpected requeue interval: %s", result.RequeueAfter)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "server-standalone-dataset-a-bootstrap"}, job); err == nil {
		t.Fatalf("expected bootstrap job to be deferred until security is ready")
	}

	updated := &fusekiv1alpha1.FusekiServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, securityReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected security condition false, got %#v", condition)
	}
}

func TestFusekiServerReconcileWithoutSecurityProfileDoesNotInjectAdminPassword(t *testing.T) {
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
			Image:       "ghcr.io/example/fuseki:6.0.0",
			DatasetRefs: []corev1.LocalObjectReference{{Name: "dataset-a"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiServer{}).
		WithObjects(server, dataset).
		Build()

	datasetReconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	reconciler := &FusekiServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone"}, deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := envVarValue(deployment.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); got != "" {
		t.Fatalf("expected no runtime admin password env var, got %q", got)
	}
	if secretName := envVarSecretRefName(deployment.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); secretName != "" {
		t.Fatalf("expected no runtime admin password secret ref, got %q", secretName)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "server-standalone-dataset-a-bootstrap"}, job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_ADMIN_PASSWORD"); got != "" {
		t.Fatalf("expected no bootstrap admin password env var, got %q", got)
	}
	if secretName := envVarSecretRefName(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_ADMIN_PASSWORD"); secretName != "" {
		t.Fatalf("expected no bootstrap admin password secret ref, got %q", secretName)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_WRITE_URL"); got != "http://standalone:3030" {
		t.Fatalf("unexpected job write url: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, securityReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "SecurityProfileNotConfigured" {
		t.Fatalf("unexpected security condition: %#v", condition)
	}
	if updated.Status.Phase != "Provisioning" {
		t.Fatalf("unexpected server phase: %q", updated.Status.Phase)
	}
}

func TestFusekiServerReconcileCreatesObservabilityResources(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	server := &fusekiv1alpha1.FusekiServer{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiServerSpec{
			Image: "ghcr.io/example/fuseki:6.0.0",
			Observability: fusekiv1alpha1.WorkloadObservabilitySpec{
				Metrics: &fusekiv1alpha1.WorkloadMetricsSpec{
					Service: fusekiv1alpha1.WorkloadMetricsServiceSpec{
						Annotations: map[string]string{"monitor": "enabled"},
					},
					ServiceMonitor: &fusekiv1alpha1.WorkloadServiceMonitorSpec{
						Interval: metav1.Duration{Duration: 30 * time.Second},
						Labels:   map[string]string{"release": "prometheus"},
					},
				},
				Logging: &fusekiv1alpha1.WorkloadLoggingSpec{PodAnnotations: map[string]string{"logs.example.com/enabled": "true"}},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiServer{}).
		WithObjects(server).
		Build()

	reconciler := &FusekiServerReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(server)}); err != nil {
		t.Fatalf("reconcile server: %v", err)
	}

	metricsService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone-metrics"}, metricsService); err != nil {
		t.Fatalf("get metrics service: %v", err)
	}
	if got := metricsService.Annotations["monitor"]; got != "enabled" {
		t.Fatalf("unexpected metrics service annotation: %q", got)
	}

	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(serviceMonitorGVK)
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone-metrics"}, serviceMonitor); err != nil {
		t.Fatalf("get service monitor: %v", err)
	}
	endpoints, found, err := unstructured.NestedSlice(serviceMonitor.Object, "spec", "endpoints")
	if err != nil || !found || len(endpoints) != 1 {
		t.Fatalf("unexpected service monitor endpoints: found=%t len=%d err=%v", found, len(endpoints), err)
	}
	endpoint, ok := endpoints[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected endpoint payload type: %T", endpoints[0])
	}
	if got := endpoint["interval"]; got != "30s" {
		t.Fatalf("unexpected service monitor interval: %v", got)
	}

	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "standalone"}, deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := deployment.Spec.Template.Annotations["logs.example.com/enabled"]; got != "true" {
		t.Fatalf("unexpected pod annotation: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiServer{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(server), updated); err != nil {
		t.Fatalf("get updated server: %v", err)
	}
	if updated.Status.MetricsServiceName != "standalone-metrics" {
		t.Fatalf("unexpected metrics service status name: %q", updated.Status.MetricsServiceName)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, monitoringReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected monitoring condition true, got %#v", condition)
	}
}
