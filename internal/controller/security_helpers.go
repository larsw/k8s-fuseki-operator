package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
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
