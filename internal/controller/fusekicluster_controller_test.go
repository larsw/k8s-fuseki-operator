package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
			OIDCIssuerURL:             "https://dex.example.com/dex",
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: "default"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: []byte("cert"), corev1.TLSPrivateKeyKey: []byte("key")}}

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
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   "example",
				"fuseki.apache.org/component": "server",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiCluster{}).
		WithObjects(cluster, pod, dataset, profile, secret, tlsSecret).
		Build()

	securityReconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := securityReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)}); err != nil {
		t.Fatalf("reconcile security profile: %v", err)
	}

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
	if got := headlessService.Spec.Ports[0].Name; got != "https" {
		t.Fatalf("unexpected headless service port name: %q", got)
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
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_OIDC_ISSUER"); got != "https://dex.example.com/dex" {
		t.Fatalf("unexpected OIDC issuer env var: %q", got)
	}
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "FUSEKI_SERVER_SCHEME"); got != "https" {
		t.Fatalf("unexpected server scheme env var: %q", got)
	}
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_SECRET"); got != "tls-secret" {
		t.Fatalf("unexpected TLS secret env var: %q", got)
	}
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_CERT_FILE"); got != securityTLSCertFile {
		t.Fatalf("unexpected TLS cert env var: %q", got)
	}
	if got := envVarValue(statefulSet.Spec.Template.Spec.Containers[0].Env, "SECURITY_PROFILE_TLS_KEY_FILE"); got != securityTLSKeyFile {
		t.Fatalf("unexpected TLS key env var: %q", got)
	}
	if secretName := envVarSecretRefName(statefulSet.Spec.Template.Spec.Containers[0].Env, "ADMIN_PASSWORD"); secretName != "admin-secret" {
		t.Fatalf("unexpected statefulset admin password secret: %q", secretName)
	}
	if mountPath := volumeMountPath(statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts, securityConfigVolumeName); mountPath != "/fuseki-extra/security" {
		t.Fatalf("unexpected security config mount: %q", mountPath)
	}
	if mountPath := volumeMountPath(statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts, securityTLSVolumeName); mountPath != securityTLSMountPath {
		t.Fatalf("unexpected security TLS mount: %q", mountPath)
	}
	if configMapName := configMapVolumeName(statefulSet.Spec.Template.Spec.Volumes, securityConfigVolumeName); configMapName != "admin-auth-security" {
		t.Fatalf("unexpected security config volume source: %q", configMapName)
	}
	if secretName := secretVolumeName(statefulSet.Spec.Template.Spec.Volumes, securityTLSVolumeName); secretName != "tls-secret" {
		t.Fatalf("unexpected security TLS volume source: %q", secretName)
	}
	for probeName, probe := range map[string]*corev1.Probe{
		"startup":   statefulSet.Spec.Template.Spec.Containers[0].StartupProbe,
		"readiness": statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe,
		"liveness":  statefulSet.Spec.Template.Spec.Containers[0].LivenessProbe,
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
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "cluster-example-dataset-a-bootstrap"}, job); err != nil {
		t.Fatalf("get bootstrap job: %v", err)
	}
	if got := envVarValue(job.Spec.Template.Spec.Containers[0].Env, "FUSEKI_WRITE_URL"); got != "https://example-write:3030" {
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

func TestFusekiClusterReconcileDefersBootstrapUntilSecurityReady(t *testing.T) {
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
	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:              "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef:  corev1.LocalObjectReference{Name: "delta"},
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "dataset-a"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "missing-profile"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   "example",
				"fuseki.apache.org/component": "server",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.Dataset{}).
		WithStatusSubresource(&fusekiv1alpha1.FusekiCluster{}).
		WithObjects(cluster, pod, dataset).
		Build()

	datasetReconciler := &DatasetReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := datasetReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dataset)}); err != nil {
		t.Fatalf("reconcile dataset: %v", err)
	}

	reconciler := &FusekiClusterReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	if err != nil {
		t.Fatalf("reconcile cluster: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("unexpected requeue interval: %s", result.RequeueAfter)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "cluster-example-dataset-a-bootstrap"}, job); err == nil {
		t.Fatalf("expected bootstrap job to be deferred until security is ready")
	}

	updated := &fusekiv1alpha1.FusekiCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatalf("get updated cluster: %v", err)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, securityReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected security condition false, got %#v", condition)
	}
}

func TestFusekiClusterReconcileCreatesObservabilityResources(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:             "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: "delta"},
			Observability: fusekiv1alpha1.WorkloadObservabilitySpec{
				Metrics: &fusekiv1alpha1.WorkloadMetricsSpec{
					Path: "/metricsz",
					Service: fusekiv1alpha1.WorkloadMetricsServiceSpec{
						Annotations: map[string]string{"monitor": "enabled"},
					},
					ServiceMonitor: &fusekiv1alpha1.WorkloadServiceMonitorSpec{
						Interval: metav1.Duration{Duration: 45 * time.Second},
						Labels:   map[string]string{"release": "prometheus"},
					},
				},
				Logging: &fusekiv1alpha1.WorkloadLoggingSpec{PodAnnotations: map[string]string{"logs.example.com/enabled": "true"}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   "example",
				"fuseki.apache.org/component": "server",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiCluster{}).
		WithObjects(cluster, pod).
		Build()

	reconciler := &FusekiClusterReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}); err != nil {
		t.Fatalf("reconcile cluster: %v", err)
	}

	metricsService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-metrics"}, metricsService); err != nil {
		t.Fatalf("get metrics service: %v", err)
	}
	if got := metricsService.Annotations["monitor"]; got != "enabled" {
		t.Fatalf("unexpected metrics service annotation: %q", got)
	}
	if got := metricsService.Spec.Selector["fuseki.apache.org/cluster"]; got != "example" {
		t.Fatalf("unexpected metrics selector: %q", got)
	}

	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(serviceMonitorGVK)
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-metrics"}, serviceMonitor); err != nil {
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
	if got := endpoint["path"]; got != "/metricsz" {
		t.Fatalf("unexpected service monitor path: %v", got)
	}
	if got := endpoint["interval"]; got != "45s" {
		t.Fatalf("unexpected service monitor interval: %v", got)
	}

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example"}, statefulSet); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if got := statefulSet.Spec.Template.Annotations["logs.example.com/enabled"]; got != "true" {
		t.Fatalf("unexpected pod annotation: %q", got)
	}

	updated := &fusekiv1alpha1.FusekiCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatalf("get updated cluster: %v", err)
	}
	if updated.Status.MetricsServiceName != "example-metrics" {
		t.Fatalf("unexpected metrics service status name: %q", updated.Status.MetricsServiceName)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, monitoringReadyConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected monitoring condition true, got %#v", condition)
	}
}

func TestFusekiClusterReconcileIgnoresBootstrapPodsForLeaseSelection(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:             "ghcr.io/example/fuseki:6.0.0",
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: "delta"},
		},
	}
	serverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   "example",
				"fuseki.apache.org/component": "server",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	bootstrapPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-example-example-dataset-bootstrap-abcde",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "fuseki",
				"fuseki.apache.org/cluster":   "example",
				"fuseki.apache.org/component": "dataset-bootstrap",
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.FusekiCluster{}).
		WithObjects(cluster, serverPod, bootstrapPod).
		Build()

	reconciler := &FusekiClusterReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}); err != nil {
		t.Fatalf("reconcile cluster: %v", err)
	}

	lease := &coordinationv1.Lease{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "example-write"}, lease); err != nil {
		t.Fatalf("get write lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "example-0" {
		t.Fatalf("unexpected lease holder: %v", lease.Spec.HolderIdentity)
	}

	updatedBootstrapPod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(bootstrapPod), updatedBootstrapPod); err != nil {
		t.Fatalf("get bootstrap pod: %v", err)
	}
	if _, ok := updatedBootstrapPod.Labels["fuseki.apache.org/read-route"]; ok {
		t.Fatalf("expected bootstrap pod to be excluded from read routing")
	}
	if _, ok := updatedBootstrapPod.Labels["fuseki.apache.org/write-route"]; ok {
		t.Fatalf("expected bootstrap pod to be excluded from write routing")
	}
	if _, ok := updatedBootstrapPod.Labels["fuseki.apache.org/lease-holder"]; ok {
		t.Fatalf("expected bootstrap pod to be excluded from lease-holder routing")
	}

	updatedServerPod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(serverPod), updatedServerPod); err != nil {
		t.Fatalf("get server pod: %v", err)
	}
	if got := updatedServerPod.Labels["fuseki.apache.org/write-route"]; got != "true" {
		t.Fatalf("unexpected server write-route label: %q", got)
	}
}
