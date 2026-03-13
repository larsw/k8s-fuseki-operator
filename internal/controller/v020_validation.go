package controller

import (
	"strconv"
	"strings"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func validateSecurityProfileSpec(profile *fusekiv1alpha1.SecurityProfile) []string {
	if profile == nil {
		return nil
	}

	issues := make([]string, 0, 4)
	switch profile.DesiredAuthorizationMode() {
	case fusekiv1alpha1.AuthorizationModeLocal:
		if profile.Spec.Authorization != nil && profile.Spec.Authorization.Ranger != nil {
			issues = append(issues, "authorization.ranger is not allowed when authorization mode is Local")
		}
	case fusekiv1alpha1.AuthorizationModeRanger:
		if profile.Spec.Authorization == nil || profile.Spec.Authorization.Ranger == nil {
			issues = append(issues, "authorization.ranger is required when authorization mode is Ranger")
			return issues
		}
		if profile.Spec.Authorization.Ranger.AdminURL == "" {
			issues = append(issues, "authorization.ranger.adminURL is required when authorization mode is Ranger")
		}
		if profile.Spec.Authorization.Ranger.ServiceName == "" {
			issues = append(issues, "authorization.ranger.serviceName is required when authorization mode is Ranger")
		}
		if profile.Spec.Authorization.Ranger.AuthSecretRef == nil || profile.Spec.Authorization.Ranger.AuthSecretRef.Name == "" {
			issues = append(issues, "authorization.ranger.authSecretRef is required when authorization mode is Ranger")
		}
	}

	return issues
}

func validateSecurityPolicySpec(policy *fusekiv1alpha1.SecurityPolicy) []string {
	if policy == nil {
		return nil
	}

	issues := make([]string, 0)
	if len(policy.Spec.Rules) == 0 {
		issues = append(issues, "spec.rules must contain at least one rule")
	}
	for i, rule := range policy.Spec.Rules {
		prefix := "spec.rules[" + itoa(i) + "]"
		if rule.Target.DatasetRef.Name == "" {
			issues = append(issues, prefix+".target.datasetRef.name is required")
		}
		if len(rule.Actions) == 0 {
			issues = append(issues, prefix+".actions must contain at least one action")
		}
		if len(rule.Subjects) == 0 {
			issues = append(issues, prefix+".subjects must contain at least one subject")
		}
		if rule.Expression == "" {
			issues = append(issues, prefix+".expression is required")
		}
		for j, subject := range rule.Subjects {
			subjectPrefix := prefix + ".subjects[" + itoa(j) + "]"
			if subject.Type == fusekiv1alpha1.SecuritySubjectTypeOIDCClaim {
				if subject.Claim == "" {
					issues = append(issues, subjectPrefix+".claim is required for OIDCClaim subjects")
				}
			} else if subject.Value == "" {
				issues = append(issues, subjectPrefix+".value is required")
			}
		}
	}

	return issues
}

func validateSHACLPolicySpec(policy *fusekiv1alpha1.SHACLPolicy) []string {
	if policy == nil {
		return nil
	}

	issues := make([]string, 0)
	if len(policy.Spec.Sources) == 0 {
		issues = append(issues, "spec.sources must contain at least one source")
	}
	for i, source := range policy.Spec.Sources {
		prefix := "spec.sources[" + itoa(i) + "]"
		switch source.Type {
		case fusekiv1alpha1.SHACLSourceTypeInline:
			if source.Inline == "" {
				issues = append(issues, prefix+".inline is required for Inline sources")
			}
		case fusekiv1alpha1.SHACLSourceTypeConfigMap:
			if source.ConfigMapRef == nil || source.ConfigMapRef.Name == "" {
				issues = append(issues, prefix+".configMapRef.name is required for ConfigMap sources")
			}
			if source.Key == "" {
				issues = append(issues, prefix+".key is required for ConfigMap sources")
			}
		}
	}

	return issues
}

func validateImportRequestSpec(request *fusekiv1alpha1.ImportRequest) []string {
	if request == nil {
		return nil
	}

	issues := validateDatasetAccessTarget("spec.target", request.Spec.Target)
	issues = append(issues, validateDataSourceSpec("spec.source", request.Spec.Source)...)
	return issues
}

func validateExportRequestSpec(request *fusekiv1alpha1.ExportRequest) []string {
	if request == nil {
		return nil
	}

	issues := validateDatasetAccessTarget("spec.target", request.Spec.Target)
	issues = append(issues, validateDataSinkSpec("spec.sink", request.Spec.Sink)...)
	return issues
}

func validateIngestPipelineSpec(pipeline *fusekiv1alpha1.IngestPipeline) []string {
	if pipeline == nil {
		return nil
	}

	issues := validateDatasetAccessTarget("spec.target", pipeline.Spec.Target)
	issues = append(issues, validateDataSourceSpec("spec.source", pipeline.Spec.Source)...)
	if pipeline.Spec.SHACLPolicyRef == nil || pipeline.Spec.SHACLPolicyRef.Name == "" {
		issues = append(issues, "spec.shaclPolicyRef.name is required")
	}
	return issues
}

func validateChangeSubscriptionSpec(subscription *fusekiv1alpha1.ChangeSubscription) []string {
	if subscription == nil {
		return nil
	}

	issues := make([]string, 0)
	if subscription.Spec.RDFDeltaServerRef.Name == "" {
		issues = append(issues, "spec.rdfDeltaServerRef.name is required")
	}
	if subscription.Spec.Target != nil {
		issues = append(issues, validateDatasetAccessTarget("spec.target", *subscription.Spec.Target)...)
	}
	issues = append(issues, validateDataSinkSpec("spec.sink", subscription.Spec.Sink)...)
	return issues
}

func validateDatasetAccessTarget(prefix string, target fusekiv1alpha1.DatasetAccessTarget) []string {
	if target.DatasetRef.Name == "" {
		return []string{prefix + ".datasetRef.name is required"}
	}
	return nil
}

func validateDataSourceSpec(prefix string, source fusekiv1alpha1.DataSourceSpec) []string {
	issues := make([]string, 0, 2)
	switch source.Type {
	case fusekiv1alpha1.DataSourceTypeFilesystem:
		if source.Path == "" {
			issues = append(issues, prefix+".path is required for Filesystem sources")
		}
		if source.URI != "" {
			issues = append(issues, prefix+".uri must be empty for Filesystem sources")
		}
	default:
		if source.URI == "" {
			issues = append(issues, prefix+".uri is required for URL and S3 sources")
		}
		if source.Path != "" {
			issues = append(issues, prefix+".path must be empty for URL and S3 sources")
		}
	}
	return issues
}

func validateDataSinkSpec(prefix string, sink fusekiv1alpha1.DataSinkSpec) []string {
	issues := make([]string, 0, 2)
	switch sink.Type {
	case fusekiv1alpha1.DataSinkTypeFilesystem:
		if sink.Path == "" {
			issues = append(issues, prefix+".path is required for Filesystem sinks")
		}
		if sink.URI != "" {
			issues = append(issues, prefix+".uri must be empty for Filesystem sinks")
		}
	default:
		if sink.URI == "" {
			issues = append(issues, prefix+".uri is required for S3 sinks")
		}
		if sink.Path != "" {
			issues = append(issues, prefix+".path must be empty for S3 sinks")
		}
	}
	return issues
}

func joinValidationIssues(issues []string) string {
	return strings.Join(issues, "; ")
}

func itoa(value int) string {
	return strconv.Itoa(value)
}