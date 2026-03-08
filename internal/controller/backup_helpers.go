package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

const (
	backupReadyConditionType        = "BackupReady"
	backupPolicyAccessKeyKey        = "accessKey"
	backupPolicySecretKeyKey        = "secretKey"
	backupPolicyRequeueInterval     = securityProfileRequeueInterval
	backupPolicyConfiguredReason    = "BackupPolicyConfigured"
	backupPolicyNotConfiguredReason = "BackupPolicyNotConfigured"
)

type backupDependencyStatus struct {
	Policy  *fusekiv1alpha1.BackupPolicy
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

func resolveBackupPolicyDependency(ctx context.Context, c client.Client, namespace string, ref *corev1.LocalObjectReference) (backupDependencyStatus, error) {
	if ref == nil || ref.Name == "" {
		return backupDependencyStatus{
			Status:  metav1.ConditionTrue,
			Reason:  backupPolicyNotConfiguredReason,
			Message: "No BackupPolicy is configured.",
		}, nil
	}

	var policy fusekiv1alpha1.BackupPolicy
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &policy); err != nil {
		return backupDependencyStatus{
			Status:  metav1.ConditionFalse,
			Reason:  "BackupPolicyNotFound",
			Message: fmt.Sprintf("Waiting for BackupPolicy %q.", ref.Name),
		}, client.IgnoreNotFound(err)
	}

	configuredCondition := apimeta.FindStatusCondition(policy.Status.Conditions, configuredConditionType)
	if configuredCondition == nil {
		return backupDependencyStatus{
			Policy:  &policy,
			Status:  metav1.ConditionFalse,
			Reason:  "BackupPolicyPending",
			Message: fmt.Sprintf("Waiting for BackupPolicy %q to report configuration status.", policy.Name),
		}, nil
	}
	if configuredCondition.Status != metav1.ConditionTrue {
		reason := configuredCondition.Reason
		if reason == "" {
			reason = "BackupPolicyPending"
		}
		message := configuredCondition.Message
		if message == "" {
			message = fmt.Sprintf("Waiting for BackupPolicy %q to become ready.", policy.Name)
		}
		return backupDependencyStatus{
			Policy:  &policy,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		}, nil
	}

	return backupDependencyStatus{
		Policy:  &policy,
		Status:  metav1.ConditionTrue,
		Reason:  backupPolicyConfiguredReason,
		Message: fmt.Sprintf("BackupPolicy %q is ready.", policy.Name),
	}, nil
}
