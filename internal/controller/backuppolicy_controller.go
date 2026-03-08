package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	cron "github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type BackupPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=backuppolicies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=backuppolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *BackupPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy fusekiv1alpha1.BackupPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	missingRefs, invalidRefs, err := r.secretReferenceIssues(ctx, &policy)
	if err != nil {
		return ctrl.Result{}, err
	}

	scheduleErr := validateBackupPolicySchedule(policy.Spec.Schedule)

	updated := policy.DeepCopy()
	updated.Status.ObservedGeneration = policy.Generation
	updated.Status.Phase = "Ready"
	conditionStatus := metav1.ConditionTrue
	conditionReason := backupPolicyConfiguredReason
	conditionMessage := "BackupPolicy schedule and secret references are valid."

	switch {
	case scheduleErr != nil:
		updated.Status.Phase = "Pending"
		conditionStatus = metav1.ConditionFalse
		conditionReason = "ScheduleInvalid"
		conditionMessage = scheduleErr.Error()
	case len(missingRefs) > 0 || len(invalidRefs) > 0:
		updated.Status.Phase = "Pending"
		conditionStatus = metav1.ConditionFalse
		switch {
		case len(missingRefs) > 0 && len(invalidRefs) == 0:
			conditionReason = "ReferencesMissing"
			conditionMessage = "Waiting for referenced secrets: " + strings.Join(missingRefs, ", ")
		case len(missingRefs) == 0 && len(invalidRefs) > 0:
			conditionReason = "ReferencesInvalid"
			conditionMessage = "Waiting for referenced secrets to contain required data: " + strings.Join(invalidRefs, ", ")
		default:
			conditionReason = "ReferencesUnresolved"
			conditionMessage = "Waiting for referenced secrets and required data: " + strings.Join(append(missingRefs, invalidRefs...), ", ")
		}
	}

	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             conditionStatus,
		Reason:             conditionReason,
		Message:            conditionMessage,
		ObservedGeneration: policy.Generation,
	})

	if !reflect.DeepEqual(policy.Status, updated.Status) {
		policy.Status = updated.Status
		if err := r.Status().Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	if len(missingRefs) > 0 || len(invalidRefs) > 0 {
		return ctrl.Result{RequeueAfter: backupPolicyRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *BackupPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.BackupPolicy{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecret)).
		Complete(r)
}

func (r *BackupPolicyReconciler) secretReferenceIssues(ctx context.Context, policy *fusekiv1alpha1.BackupPolicy) ([]string, []string, error) {
	if policy.Spec.S3.CredentialsSecretRef.Name == "" {
		return nil, []string{"s3.credentialsSecretRef is required"}, nil
	}

	var secret corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Namespace: policy.Namespace, Name: policy.Spec.S3.CredentialsSecretRef.Name}, &secret)
	if err == nil {
		missingKeys := missingSecretKeys(&secret, []string{backupPolicyAccessKeyKey, backupPolicySecretKeyKey})
		if len(missingKeys) > 0 {
			return nil, []string{fmt.Sprintf("s3.credentialsSecretRef/%s missing keys: %s", policy.Spec.S3.CredentialsSecretRef.Name, strings.Join(missingKeys, ", "))}, nil
		}
		return nil, nil, nil
	}
	if apierrors.IsNotFound(err) {
		return []string{fmt.Sprintf("s3.credentialsSecretRef/%s", policy.Spec.S3.CredentialsSecretRef.Name)}, nil, nil
	}
	return nil, nil, err
}

func (r *BackupPolicyReconciler) requestsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var policies fusekiv1alpha1.BackupPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range policies.Items {
		policy := &policies.Items[i]
		if policy.Spec.S3.CredentialsSecretRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	}

	return requests
}

func validateBackupPolicySchedule(schedule string) error {
	if schedule == "" {
		return fmt.Errorf("backup schedule must be specified")
	}
	if _, err := cron.ParseStandard(schedule); err != nil {
		return fmt.Errorf("invalid backup schedule %q: %w", schedule, err)
	}
	return nil
}
