package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type datasetAdmissionValidator struct{ client.Client }
type fusekiClusterAdmissionValidator struct{ client.Client }
type fusekiServerAdmissionValidator struct{ client.Client }
type securityPolicyAdmissionValidator struct{ client.Client }
type importRequestAdmissionValidator struct{ client.Client }
type exportRequestAdmissionValidator struct{ client.Client }
type ingestPipelineAdmissionValidator struct{ client.Client }
type changeSubscriptionAdmissionValidator struct{ client.Client }

func SetupV020Webhooks(mgr ctrl.Manager) error {
	validators := []func() error{
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.Dataset{}).
				WithValidator(&datasetAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-dataset").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.FusekiCluster{}).
				WithValidator(&fusekiClusterAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-fusekicluster").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.FusekiServer{}).
				WithValidator(&fusekiServerAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-fusekiserver").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.SecurityPolicy{}).
				WithValidator(&securityPolicyAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-securitypolicy").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.ImportRequest{}).
				WithValidator(&importRequestAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-importrequest").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.ExportRequest{}).
				WithValidator(&exportRequestAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-exportrequest").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.IngestPipeline{}).
				WithValidator(&ingestPipelineAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-ingestpipeline").
				Complete()
		},
		func() error {
			return ctrl.NewWebhookManagedBy(mgr, &fusekiv1alpha1.ChangeSubscription{}).
				WithValidator(&changeSubscriptionAdmissionValidator{Client: mgr.GetClient()}).
				WithValidatorCustomPath("/validate-fuseki-apache-org-v1alpha1-changesubscription").
				Complete()
		},
	}

	for _, register := range validators {
		if err := register(); err != nil {
			return err
		}
	}

	return nil
}

func (v *datasetAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.Dataset) (admission.Warnings, error) {
	return nil, validateDatasetAdmission(ctx, v.Client, obj)
}

func (v *datasetAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.Dataset) (admission.Warnings, error) {
	return nil, validateDatasetAdmission(ctx, v.Client, obj)
}

func (v *datasetAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.Dataset) (admission.Warnings, error) {
	return nil, nil
}

func (v *fusekiClusterAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.FusekiCluster) (admission.Warnings, error) {
	return nil, validateFusekiClusterAdmission(ctx, v.Client, obj)
}

func (v *fusekiClusterAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.FusekiCluster) (admission.Warnings, error) {
	return nil, validateFusekiClusterAdmission(ctx, v.Client, obj)
}

func (v *fusekiClusterAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.FusekiCluster) (admission.Warnings, error) {
	return nil, nil
}

func (v *fusekiServerAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.FusekiServer) (admission.Warnings, error) {
	return nil, validateFusekiServerAdmission(ctx, v.Client, obj)
}

func (v *fusekiServerAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.FusekiServer) (admission.Warnings, error) {
	return nil, validateFusekiServerAdmission(ctx, v.Client, obj)
}

func (v *fusekiServerAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.FusekiServer) (admission.Warnings, error) {
	return nil, nil
}

func (v *securityPolicyAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.SecurityPolicy) (admission.Warnings, error) {
	return nil, validateSecurityPolicyAdmission(ctx, v.Client, obj)
}

func (v *securityPolicyAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.SecurityPolicy) (admission.Warnings, error) {
	return nil, validateSecurityPolicyAdmission(ctx, v.Client, obj)
}

func (v *securityPolicyAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.SecurityPolicy) (admission.Warnings, error) {
	return nil, nil
}

func (v *importRequestAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.ImportRequest) (admission.Warnings, error) {
	return nil, validateImportRequestAdmission(ctx, v.Client, obj)
}

func (v *importRequestAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.ImportRequest) (admission.Warnings, error) {
	return nil, validateImportRequestAdmission(ctx, v.Client, obj)
}

func (v *importRequestAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.ImportRequest) (admission.Warnings, error) {
	return nil, nil
}

func (v *exportRequestAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.ExportRequest) (admission.Warnings, error) {
	return nil, validateExportRequestAdmission(ctx, v.Client, obj)
}

func (v *exportRequestAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.ExportRequest) (admission.Warnings, error) {
	return nil, validateExportRequestAdmission(ctx, v.Client, obj)
}

func (v *exportRequestAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.ExportRequest) (admission.Warnings, error) {
	return nil, nil
}

func (v *ingestPipelineAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.IngestPipeline) (admission.Warnings, error) {
	return nil, validateIngestPipelineAdmission(ctx, v.Client, obj)
}

func (v *ingestPipelineAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.IngestPipeline) (admission.Warnings, error) {
	return nil, validateIngestPipelineAdmission(ctx, v.Client, obj)
}

func (v *ingestPipelineAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.IngestPipeline) (admission.Warnings, error) {
	return nil, nil
}

func (v *changeSubscriptionAdmissionValidator) ValidateCreate(ctx context.Context, obj *fusekiv1alpha1.ChangeSubscription) (admission.Warnings, error) {
	return nil, validateChangeSubscriptionAdmission(ctx, v.Client, obj)
}

func (v *changeSubscriptionAdmissionValidator) ValidateUpdate(ctx context.Context, _, obj *fusekiv1alpha1.ChangeSubscription) (admission.Warnings, error) {
	return nil, validateChangeSubscriptionAdmission(ctx, v.Client, obj)
}

func (v *changeSubscriptionAdmissionValidator) ValidateDelete(context.Context, *fusekiv1alpha1.ChangeSubscription) (admission.Warnings, error) {
	return nil, nil
}

func validateDatasetAdmission(ctx context.Context, c client.Client, dataset *fusekiv1alpha1.Dataset) error {
	issues := make([]string, 0)
	for i, ref := range dataset.Spec.SecurityPolicies {
		if ref.Name == "" {
			issues = append(issues, fmt.Sprintf("spec.securityPolicies[%d].name is required", i))
			continue
		}
		if err := c.Get(ctx, client.ObjectKey{Namespace: dataset.Namespace, Name: ref.Name}, &fusekiv1alpha1.SecurityPolicy{}); err != nil {
			issues = append(issues, fmt.Sprintf("spec.securityPolicies[%d].name references missing SecurityPolicy %q", i, ref.Name))
		}
	}
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("Dataset"), dataset.Name, issues)
}

func validateFusekiClusterAdmission(ctx context.Context, c client.Client, cluster *fusekiv1alpha1.FusekiCluster) error {
	issues := validateFusekiWorkloadSecurity(ctx, c, cluster.Namespace, cluster.Spec.SecurityProfileRef, cluster.Spec.DatasetRefs)
	if cluster.Spec.RDFDeltaServerRef.Name == "" {
		issues = append(issues, "spec.rdfDeltaServerRef.name is required")
	} else if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.RDFDeltaServerRef.Name}, &fusekiv1alpha1.RDFDeltaServer{}); err != nil {
		issues = append(issues, fmt.Sprintf("spec.rdfDeltaServerRef.name references missing RDFDeltaServer %q", cluster.Spec.RDFDeltaServerRef.Name))
	}
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("FusekiCluster"), cluster.Name, issues)
}

func validateFusekiServerAdmission(ctx context.Context, c client.Client, server *fusekiv1alpha1.FusekiServer) error {
	issues := validateFusekiWorkloadSecurity(ctx, c, server.Namespace, server.Spec.SecurityProfileRef, server.Spec.DatasetRefs)
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("FusekiServer"), server.Name, issues)
}

func validateSecurityPolicyAdmission(ctx context.Context, c client.Client, policy *fusekiv1alpha1.SecurityPolicy) error {
	issues := validateSecurityPolicySpec(policy)
	for i, rule := range policy.Spec.Rules {
		if rule.Target.DatasetRef.Name == "" {
			continue
		}
		if err := c.Get(ctx, client.ObjectKey{Namespace: policy.Namespace, Name: rule.Target.DatasetRef.Name}, &fusekiv1alpha1.Dataset{}); err != nil {
			issues = append(issues, fmt.Sprintf("spec.rules[%d].target.datasetRef.name references missing Dataset %q", i, rule.Target.DatasetRef.Name))
		}
	}
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("SecurityPolicy"), policy.Name, issues)
}

func validateImportRequestAdmission(ctx context.Context, c client.Client, request *fusekiv1alpha1.ImportRequest) error {
	issues := validateImportRequestSpec(request)
	issues = append(issues, validateDatasetExists(ctx, c, request.Namespace, "spec.target.datasetRef.name", request.Spec.Target.DatasetRef.Name)...)
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("ImportRequest"), request.Name, issues)
}

func validateExportRequestAdmission(ctx context.Context, c client.Client, request *fusekiv1alpha1.ExportRequest) error {
	issues := validateExportRequestSpec(request)
	issues = append(issues, validateDatasetExists(ctx, c, request.Namespace, "spec.target.datasetRef.name", request.Spec.Target.DatasetRef.Name)...)
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("ExportRequest"), request.Name, issues)
}

func validateIngestPipelineAdmission(ctx context.Context, c client.Client, pipeline *fusekiv1alpha1.IngestPipeline) error {
	issues := validateIngestPipelineSpec(pipeline)
	issues = append(issues, validateDatasetExists(ctx, c, pipeline.Namespace, "spec.target.datasetRef.name", pipeline.Spec.Target.DatasetRef.Name)...)
	if pipeline.Spec.SHACLPolicyRef != nil && pipeline.Spec.SHACLPolicyRef.Name != "" {
		if err := c.Get(ctx, client.ObjectKey{Namespace: pipeline.Namespace, Name: pipeline.Spec.SHACLPolicyRef.Name}, &fusekiv1alpha1.SHACLPolicy{}); err != nil {
			issues = append(issues, fmt.Sprintf("spec.shaclPolicyRef.name references missing SHACLPolicy %q", pipeline.Spec.SHACLPolicyRef.Name))
		}
	}
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("IngestPipeline"), pipeline.Name, issues)
}

func validateChangeSubscriptionAdmission(ctx context.Context, c client.Client, subscription *fusekiv1alpha1.ChangeSubscription) error {
	issues := validateChangeSubscriptionSpec(subscription)
	if subscription.Spec.RDFDeltaServerRef.Name != "" {
		if err := c.Get(ctx, client.ObjectKey{Namespace: subscription.Namespace, Name: subscription.Spec.RDFDeltaServerRef.Name}, &fusekiv1alpha1.RDFDeltaServer{}); err != nil {
			issues = append(issues, fmt.Sprintf("spec.rdfDeltaServerRef.name references missing RDFDeltaServer %q", subscription.Spec.RDFDeltaServerRef.Name))
		}
	}
	if subscription.Spec.Target != nil {
		issues = append(issues, validateDatasetExists(ctx, c, subscription.Namespace, "spec.target.datasetRef.name", subscription.Spec.Target.DatasetRef.Name)...)
	}
	return invalidIfNeeded(fusekiv1alpha1.GroupVersion.WithKind("ChangeSubscription"), subscription.Name, issues)
}

func validateFusekiWorkloadSecurity(ctx context.Context, c client.Client, namespace string, profileRef *corev1.LocalObjectReference, datasetRefs []corev1.LocalObjectReference) []string {
	issues := make([]string, 0)
	rangerMode := false

	if profileRef != nil && profileRef.Name != "" {
		var profile fusekiv1alpha1.SecurityProfile
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: profileRef.Name}, &profile); err != nil {
			issues = append(issues, fmt.Sprintf("spec.securityProfileRef.name references missing SecurityProfile %q", profileRef.Name))
		} else {
			rangerMode = profile.DesiredAuthorizationMode() == fusekiv1alpha1.AuthorizationModeRanger
		}
	}

	for i, ref := range datasetRefs {
		if ref.Name == "" {
			issues = append(issues, fmt.Sprintf("spec.datasetRefs[%d].name is required", i))
			continue
		}

		var dataset fusekiv1alpha1.Dataset
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &dataset); err != nil {
			issues = append(issues, fmt.Sprintf("spec.datasetRefs[%d].name references missing Dataset %q", i, ref.Name))
			continue
		}

		if rangerMode && len(dataset.Spec.SecurityPolicies) > 0 {
			issues = append(issues, fmt.Sprintf("spec.datasetRefs[%d].name references Dataset %q with local securityPolicies, which is not allowed in Ranger authorization mode", i, ref.Name))
		}
	}

	return issues
}

func validateDatasetExists(ctx context.Context, c client.Client, namespace, fieldPath, name string) []string {
	if name == "" {
		return nil
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &fusekiv1alpha1.Dataset{}); err != nil {
		return []string{fmt.Sprintf("%s references missing Dataset %q", fieldPath, name)}
	}
	return nil
}

func invalidIfNeeded(gvk schema.GroupVersionKind, name string, issues []string) error {
	if len(issues) == 0 {
		return nil
	}
	status := metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonInvalid,
		Message: joinValidationIssues(issues),
		Details: &metav1.StatusDetails{Group: gvk.Group, Kind: gvk.Kind, Name: name},
	}
	return &apierrors.StatusError{ErrStatus: status}
}
