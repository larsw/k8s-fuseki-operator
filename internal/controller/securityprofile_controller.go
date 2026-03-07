package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

const securityProfileRequeueInterval = 15 * time.Second

type SecurityProfileReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securityprofiles,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securityprofiles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *SecurityProfileReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var profile fusekiv1alpha1.SecurityProfile
	if err := r.Get(ctx, req.NamespacedName, &profile); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	missingRefs, err := r.missingSecretRefs(ctx, &profile)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := profile.DeepCopy()
	updated.Status.ObservedGeneration = profile.Generation
	updated.Status.Phase = "Ready"
	conditionStatus := metav1.ConditionTrue
	conditionReason := "ReferencesResolved"
	conditionMessage := "All referenced secrets are available."
	if len(missingRefs) > 0 {
		updated.Status.Phase = "Pending"
		conditionStatus = metav1.ConditionFalse
		conditionReason = "ReferencesMissing"
		conditionMessage = "Waiting for referenced secrets: " + strings.Join(missingRefs, ", ")
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             conditionStatus,
		Reason:             conditionReason,
		Message:            conditionMessage,
		ObservedGeneration: profile.Generation,
	})

	if !reflect.DeepEqual(profile.Status, updated.Status) {
		profile.Status = updated.Status
		if err := r.Status().Update(ctx, &profile); err != nil {
			return ctrl.Result{}, err
		}
	}

	if len(missingRefs) > 0 {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SecurityProfileReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.SecurityProfile{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecret)).
		Complete(r)
}

func (r *SecurityProfileReconciler) missingSecretRefs(ctx context.Context, profile *fusekiv1alpha1.SecurityProfile) ([]string, error) {
	checks := []struct {
		label string
		ref   *corev1.LocalObjectReference
	}{
		{label: "adminCredentialsSecretRef", ref: profile.Spec.AdminCredentialsSecretRef},
		{label: "tlsSecretRef", ref: profile.Spec.TLSSecretRef},
	}

	missing := make([]string, 0, len(checks))
	for _, check := range checks {
		if check.ref == nil || check.ref.Name == "" {
			continue
		}

		var secret corev1.Secret
		err := r.Get(ctx, client.ObjectKey{Namespace: profile.Namespace, Name: check.ref.Name}, &secret)
		if err == nil {
			continue
		}
		if apierrors.IsNotFound(err) {
			missing = append(missing, fmt.Sprintf("%s/%s", check.label, check.ref.Name))
			continue
		}
		return nil, err
	}

	sort.Strings(missing)
	return missing, nil
}

func (r *SecurityProfileReconciler) requestsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var profiles fusekiv1alpha1.SecurityProfileList
	if err := r.List(ctx, &profiles, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	seen := map[string]struct{}{}
	for i := range profiles.Items {
		profile := &profiles.Items[i]
		if !securityProfileReferencesSecret(profile, obj.GetName()) {
			continue
		}
		key := fmt.Sprintf("%s/%s", profile.Namespace, profile.Name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	}

	return requests
}

func securityProfileReferencesSecret(profile *fusekiv1alpha1.SecurityProfile, secretName string) bool {
	for _, ref := range []*corev1.LocalObjectReference{profile.Spec.AdminCredentialsSecretRef, profile.Spec.TLSSecretRef} {
		if ref != nil && ref.Name == secretName {
			return true
		}
	}
	return false
}
