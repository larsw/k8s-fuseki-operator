package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestValidateDatasetAdmissionRejectsMissingSecurityPolicy(t *testing.T) {
	t.Helper()

	k8sClient := fake.NewClientBuilder().
		WithScheme(newWebhookTestScheme(t)).
		Build()

	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "missing-policy"}},
		},
	}

	err := validateDatasetAdmission(context.Background(), k8sClient, dataset)
	assertInvalidErrorContains(t, err, `missing SecurityPolicy "missing-policy"`)
}

func TestValidateFusekiClusterAdmissionRejectsLocalPoliciesInRangerMode(t *testing.T) {
	t.Helper()

	scheme := newWebhookTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "example-securitypolicy"}},
		},
	}
	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "example-securityprofile-ranger", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			Authorization: &fusekiv1alpha1.SecurityAuthorizationSpec{
				Mode: fusekiv1alpha1.AuthorizationModeRanger,
				Ranger: &fusekiv1alpha1.RangerAuthorizationSpec{
					AdminURL:      "https://ranger.example.com",
					ServiceName:   "fuseki-default",
					AuthSecretRef: &corev1.LocalObjectReference{Name: "ranger-auth"},
				},
			},
		},
	}
	rdfDelta := &fusekiv1alpha1.RDFDeltaServer{ObjectMeta: metav1.ObjectMeta{Name: "example-delta", Namespace: "default"}}
	cluster := &fusekiv1alpha1.FusekiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: fusekiv1alpha1.FusekiClusterSpec{
			Image:              "ghcr.io/larsw/k8s-fuseki-operator/fuseki:6.0.0",
			RDFDeltaServerRef:  corev1.LocalObjectReference{Name: "example-delta"},
			DatasetRefs:        []corev1.LocalObjectReference{{Name: "example-dataset"}},
			SecurityProfileRef: &corev1.LocalObjectReference{Name: "example-securityprofile-ranger"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dataset, profile, rdfDelta).
		Build()

	err := validateFusekiClusterAdmission(context.Background(), k8sClient, cluster)
	assertInvalidErrorContains(t, err, "with local securityPolicies, which is not allowed in Ranger authorization mode")
}

func TestValidateIngestPipelineAdmissionRejectsMissingSHACLPolicy(t *testing.T) {
	t.Helper()

	scheme := newWebhookTestScheme(t)
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec:       fusekiv1alpha1.DatasetSpec{Name: "primary"},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-ingest", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target: fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "example-dataset"}},
			Source: fusekiv1alpha1.DataSourceSpec{
				Type:   fusekiv1alpha1.DataSourceTypeURL,
				URI:    "https://data.example.com/latest.nq",
				Format: "application/n-quads",
			},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: "missing-shacl"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dataset).
		Build()

	err := validateIngestPipelineAdmission(context.Background(), k8sClient, pipeline)
	assertInvalidErrorContains(t, err, `missing SHACLPolicy "missing-shacl"`)
}

func TestV020DefaultHelpers(t *testing.T) {
	t.Helper()

	rule := fusekiv1alpha1.SecurityPolicyRule{}
	if got := rule.DesiredEffect(); got != fusekiv1alpha1.SecurityPolicyEffectAllow {
		t.Fatalf("unexpected default effect: %q", got)
	}
	if got := rule.DesiredExpressionType(); got != fusekiv1alpha1.SecurityPolicyExpressionTypeSimple {
		t.Fatalf("unexpected default expression type: %q", got)
	}

	policy := &fusekiv1alpha1.SHACLPolicy{}
	if got := policy.DesiredFailureAction(); got != fusekiv1alpha1.SHACLFailureActionReject {
		t.Fatalf("unexpected default failure action: %q", got)
	}
}

func newWebhookTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	return scheme
}

func assertInvalidErrorContains(t *testing.T, err error, want string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected invalid error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("unexpected error %q, want substring %q", err.Error(), want)
	}
}
