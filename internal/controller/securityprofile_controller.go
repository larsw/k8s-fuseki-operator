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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

const securityProfileRequeueInterval = 15 * time.Second

type SecurityProfileReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securityprofiles,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securityprofiles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *SecurityProfileReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var profile fusekiv1alpha1.SecurityProfile
	if err := r.Get(ctx, req.NamespacedName, &profile); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileConfigMap(ctx, &profile); err != nil {
		return ctrl.Result{}, err
	}

	specIssues := validateSecurityProfileSpec(&profile)
	missingRefs, invalidRefs, err := r.secretReferenceIssues(ctx, &profile)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := profile.DeepCopy()
	updated.Status.ObservedGeneration = profile.Generation
	updated.Status.Phase = "Ready"
	updated.Status.ConfigMapName = profile.ConfigMapName()
	updated.Status.AuthorizationMode = profile.DesiredAuthorizationMode()
	conditionStatus := metav1.ConditionTrue
	conditionReason := "ReferencesResolved"
	conditionMessage := "All referenced secrets are available."
	if len(specIssues) > 0 {
		updated.Status.Phase = "Invalid"
		conditionStatus = metav1.ConditionFalse
		conditionReason = "InvalidSpec"
		conditionMessage = joinValidationIssues(specIssues)
	} else if len(missingRefs) > 0 || len(invalidRefs) > 0 {
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
		ObservedGeneration: profile.Generation,
	})

	if !reflect.DeepEqual(profile.Status, updated.Status) {
		profile.Status = updated.Status
		if err := r.Status().Update(ctx, &profile); err != nil {
			return ctrl.Result{}, err
		}
	}

	if len(specIssues) > 0 {
		return ctrl.Result{}, nil
	}

	if len(missingRefs) > 0 || len(invalidRefs) > 0 {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SecurityProfileReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.SecurityProfile{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSecret)).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func (r *SecurityProfileReconciler) reconcileConfigMap(ctx context.Context, profile *fusekiv1alpha1.SecurityProfile) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: profile.ConfigMapName(), Namespace: profile.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = map[string]string{
			"app.kubernetes.io/name":       "fuseki-security-profile",
			"app.kubernetes.io/instance":   profile.Name,
			"app.kubernetes.io/managed-by": "fuseki-operator",
			"fuseki.apache.org/security":   profile.Name,
		}
		configMap.Data = renderSecurityProfileConfigData(profile)
		return controllerutil.SetControllerReference(profile, configMap, r.Scheme)
	})
	return err
}

func (r *SecurityProfileReconciler) secretReferenceIssues(ctx context.Context, profile *fusekiv1alpha1.SecurityProfile) ([]string, []string, error) {
	checks := []struct {
		label        string
		ref          *corev1.LocalObjectReference
		requiredKeys []string
	}{
		{label: "adminCredentialsSecretRef", ref: profile.Spec.AdminCredentialsSecretRef},
		{label: "tlsSecretRef", ref: profile.Spec.TLSSecretRef, requiredKeys: []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey}},
	}

	missing := make([]string, 0, len(checks))
	invalid := make([]string, 0, len(checks)+2)
	if profile.RangerAuthorizationEnabled() {
		if profile.Spec.Authorization.Ranger.AdminURL == "" {
			invalid = append(invalid, "authorization.ranger.adminURL must be set when authorization mode is Ranger")
		}
		if profile.Spec.Authorization.Ranger.ServiceName == "" {
			invalid = append(invalid, "authorization.ranger.serviceName must be set when authorization mode is Ranger")
		}
		checks = append(checks, struct {
			label        string
			ref          *corev1.LocalObjectReference
			requiredKeys []string
		}{
			label:        "authorization.ranger.authSecretRef",
			ref:          profile.Spec.Authorization.Ranger.AuthSecretRef,
			requiredKeys: []string{"username", "password"},
		})
	}
	for _, check := range checks {
		if check.ref == nil || check.ref.Name == "" {
			continue
		}

		var secret corev1.Secret
		err := r.Get(ctx, client.ObjectKey{Namespace: profile.Namespace, Name: check.ref.Name}, &secret)
		if err == nil {
			missingKeys := missingSecretKeys(&secret, check.requiredKeys)
			if len(missingKeys) > 0 {
				invalid = append(invalid, fmt.Sprintf("%s/%s missing keys: %s", check.label, check.ref.Name, strings.Join(missingKeys, ", ")))
			}
			continue
		}
		if apierrors.IsNotFound(err) {
			missing = append(missing, fmt.Sprintf("%s/%s", check.label, check.ref.Name))
			continue
		}
		return nil, nil, err
	}

	sort.Strings(missing)
	sort.Strings(invalid)
	return missing, invalid, nil
}

func missingSecretKeys(secret *corev1.Secret, requiredKeys []string) []string {
	if len(requiredKeys) == 0 {
		return nil
	}

	missing := make([]string, 0, len(requiredKeys))
	for _, key := range requiredKeys {
		if len(secret.Data[key]) == 0 {
			missing = append(missing, key)
		}
	}
	return missing
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
	refs := []*corev1.LocalObjectReference{profile.Spec.AdminCredentialsSecretRef, profile.Spec.TLSSecretRef}
	if profile.RangerAuthorizationEnabled() {
		refs = append(refs, profile.Spec.Authorization.Ranger.AuthSecretRef)
	}
	for _, ref := range refs {
		if ref != nil && ref.Name == secretName {
			return true
		}
	}
	return false
}
