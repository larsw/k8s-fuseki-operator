package controller

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

const securityReadyConditionType = "SecurityReady"

type securityDependencyStatus struct {
	Profile        *fusekiv1alpha1.SecurityProfile
	AdminSecretRef *corev1.LocalObjectReference
	Status         metav1.ConditionStatus
	Reason         string
	Message        string
}

func resolveSecurityDependency(ctx context.Context, c client.Client, namespace string, securityProfileRef *corev1.LocalObjectReference) (securityDependencyStatus, error) {
	if securityProfileRef == nil || securityProfileRef.Name == "" {
		return securityDependencyStatus{
			Status:  metav1.ConditionTrue,
			Reason:  "SecurityProfileNotConfigured",
			Message: "No SecurityProfile is referenced.",
		}, nil
	}

	var profile fusekiv1alpha1.SecurityProfile
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: securityProfileRef.Name}, &profile); err != nil {
		if apierrors.IsNotFound(err) {
			return securityDependencyStatus{
				Status:  metav1.ConditionFalse,
				Reason:  "SecurityProfileNotFound",
				Message: fmt.Sprintf("Waiting for SecurityProfile %q.", securityProfileRef.Name),
			}, nil
		}
		return securityDependencyStatus{}, err
	}

	configuredCondition := apimeta.FindStatusCondition(profile.Status.Conditions, configuredConditionType)
	if configuredCondition == nil || configuredCondition.Status != metav1.ConditionTrue {
		message := fmt.Sprintf("Waiting for SecurityProfile %q to resolve its references.", profile.Name)
		if configuredCondition != nil && configuredCondition.Message != "" {
			message = configuredCondition.Message
		}
		return securityDependencyStatus{
			Profile:        &profile,
			AdminSecretRef: profile.Spec.AdminCredentialsSecretRef,
			Status:         metav1.ConditionFalse,
			Reason:         "SecurityProfileNotReady",
			Message:        message,
		}, nil
	}

	return securityDependencyStatus{
		Profile:        &profile,
		AdminSecretRef: profile.Spec.AdminCredentialsSecretRef,
		Status:         metav1.ConditionTrue,
		Reason:         "SecurityProfileReady",
		Message:        fmt.Sprintf("SecurityProfile %q is ready.", profile.Name),
	}, nil
}

func resolveFusekiWorkloadSecurityDependency(ctx context.Context, c client.Client, namespace string, securityProfileRef *corev1.LocalObjectReference, datasetRefs []corev1.LocalObjectReference) (securityDependencyStatus, error) {
	status, err := resolveSecurityDependency(ctx, c, namespace, securityProfileRef)
	if err != nil {
		return securityDependencyStatus{}, err
	}
	if status.Status != metav1.ConditionTrue || status.Profile == nil || !status.Profile.RangerAuthorizationEnabled() {
		return status, nil
	}

	datasetsWithLocalPolicies, err := rangerLocalPolicyDatasets(ctx, c, namespace, datasetRefs)
	if err != nil {
		return securityDependencyStatus{}, err
	}
	if len(datasetsWithLocalPolicies) == 0 {
		return status, nil
	}

	return securityDependencyStatus{
		Profile:        status.Profile,
		AdminSecretRef: status.AdminSecretRef,
		Status:         metav1.ConditionFalse,
		Reason:         "RangerLocalPoliciesUnsupported",
		Message: fmt.Sprintf(
			"SecurityProfile %q uses Ranger authorization and cannot consume local SecurityPolicy attachments on Datasets: %s.",
			status.Profile.Name,
			joinQuotedNames(datasetsWithLocalPolicies),
		),
	}, nil
}

func workloadSecurityReady(status securityDependencyStatus) bool {
	return status.Status == metav1.ConditionTrue
}

func rangerLocalPolicyDatasets(ctx context.Context, c client.Client, namespace string, datasetRefs []corev1.LocalObjectReference) ([]string, error) {
	seen := make(map[string]struct{}, len(datasetRefs))
	names := make([]string, 0)
	for _, ref := range datasetRefs {
		if ref.Name == "" {
			continue
		}
		if _, ok := seen[ref.Name]; ok {
			continue
		}
		seen[ref.Name] = struct{}{}

		var dataset fusekiv1alpha1.Dataset
		err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &dataset)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if len(dataset.Spec.SecurityPolicies) == 0 {
			continue
		}
		names = append(names, dataset.Name)
	}
	sort.Strings(names)
	return names, nil
}

func joinQuotedNames(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	return fmt.Sprintf("[%s]", joinWithCommaSpace(quoted))
}

func joinWithCommaSpace(values []string) string {
	result := ""
	for index, value := range values {
		if index > 0 {
			result += ", "
		}
		result += value
	}
	return result
}
