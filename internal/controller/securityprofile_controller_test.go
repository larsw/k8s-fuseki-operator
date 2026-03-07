package controller

import (
	"context"
	"strings"
	"testing"

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

func TestSecurityProfileReconcileReady(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
			TLSSecretRef:              &corev1.LocalObjectReference{Name: "tls-secret"},
			OIDCIssuerURL:             "https://dex.example.com/dex",
		},
	}
	adminSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "admin-secret", Namespace: "default"}}
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       []byte("cert"),
			corev1.TLSPrivateKeyKey: []byte("key"),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithObjects(profile, adminSecret, tlsSecret).
		Build()

	reconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(profile), updated); err != nil {
		t.Fatalf("get updated profile: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected configured condition true, got %#v", condition)
	}
	if updated.Status.ObservedGeneration != profile.Generation {
		t.Fatalf("unexpected observed generation: %d", updated.Status.ObservedGeneration)
	}
	if updated.Status.ConfigMapName != "secure-security" {
		t.Fatalf("unexpected configmap status name: %q", updated.Status.ConfigMapName)
	}

	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "secure-security"}, configMap); err != nil {
		t.Fatalf("get projected configmap: %v", err)
	}
	if got := configMap.Data["security.properties"]; got == "" || !containsLine(got, "oidc.issuerURL=https://dex.example.com/dex") || !containsLine(got, "tls.certFile=/fuseki-extra/security/tls/tls.crt") || !containsLine(got, "tls.keyFile=/fuseki-extra/security/tls/tls.key") {
		t.Fatalf("unexpected security properties: %q", got)
	}
}

func TestSecurityProfileReconcilePendingWhenTLSSecretInvalid(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			TLSSecretRef: &corev1.LocalObjectReference{Name: "tls-secret"},
		},
	}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tls-secret", Namespace: "default"}, Type: corev1.SecretTypeTLS}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithObjects(profile, tlsSecret).
		Build()

	reconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("expected %s requeue, got %s", securityProfileRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(profile), updated); err != nil {
		t.Fatalf("get updated profile: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected configured condition false, got %#v", condition)
	}
	if condition.Reason != "ReferencesInvalid" {
		t.Fatalf("unexpected configured condition reason: %#v", condition)
	}
	if !strings.Contains(condition.Message, "tls.crt") || !strings.Contains(condition.Message, "tls.key") {
		t.Fatalf("unexpected configured condition message: %q", condition.Message)
	}
}

func TestSecurityProfileReconcilePendingWhenSecretMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			AdminCredentialsSecretRef: &corev1.LocalObjectReference{Name: "admin-secret"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithObjects(profile).
		Build()

	reconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != securityProfileRequeueInterval {
		t.Fatalf("expected 15s requeue, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(profile), updated); err != nil {
		t.Fatalf("get updated profile: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected configured condition false, got %#v", condition)
	}
	if condition.Reason != "ReferencesMissing" {
		t.Fatalf("unexpected configured condition reason: %#v", condition)
	}
	if updated.Status.ConfigMapName != "secure-security" {
		t.Fatalf("unexpected configmap status name: %q", updated.Status.ConfigMapName)
	}
}
