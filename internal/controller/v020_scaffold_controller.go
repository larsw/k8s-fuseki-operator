package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type SecurityPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type SHACLPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type ImportRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type ExportRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type IngestPipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type ChangeSubscriptionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securitypolicies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=securitypolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets,verbs=get;list;watch
func (r *SecurityPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy fusekiv1alpha1.SecurityPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	specIssues := validateSecurityPolicySpec(&policy)
	missingTargets, attachedDatasets, err := r.policyDependencies(ctx, &policy)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := policy.DeepCopy()
	updated.Status.ObservedGeneration = policy.Generation
	updated.Status.Phase = "Ready"
	condition := metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "DependenciesResolved",
		Message:            r.securityPolicyReadyMessage(&policy, attachedDatasets),
		ObservedGeneration: policy.Generation,
	}
	if len(specIssues) > 0 {
		updated.Status.Phase = "Invalid"
		condition.Status = metav1.ConditionFalse
		condition.Reason = "InvalidSpec"
		condition.Message = joinValidationIssues(specIssues)
	} else if len(missingTargets) > 0 {
		updated.Status.Phase = "Pending"
		condition.Status = metav1.ConditionFalse
		condition.Reason = "DependenciesMissing"
		condition.Message = "Waiting for target Datasets: " + strings.Join(missingTargets, ", ")
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, condition)

	if !reflect.DeepEqual(policy.Status, updated.Status) {
		policy.Status = updated.Status
		if err := r.Status().Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *SecurityPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.SecurityPolicy{}).
		Watches(&fusekiv1alpha1.Dataset{}, handler.EnqueueRequestsFromMapFunc(r.requestsForDataset)).
		Complete(r)
}

func (r *SecurityPolicyReconciler) policyDependencies(ctx context.Context, policy *fusekiv1alpha1.SecurityPolicy) ([]string, []string, error) {
	targetNames := securityPolicyTargetDatasetNames(policy)
	missingTargets := make([]string, 0)
	for _, name := range targetNames {
		var dataset fusekiv1alpha1.Dataset
		if err := r.Get(ctx, client.ObjectKey{Namespace: policy.Namespace, Name: name}, &dataset); err != nil {
			if client.IgnoreNotFound(err) == nil {
				missingTargets = append(missingTargets, name)
				continue
			}
			return nil, nil, err
		}
	}

	var datasets fusekiv1alpha1.DatasetList
	if err := r.List(ctx, &datasets, client.InNamespace(policy.Namespace)); err != nil {
		return nil, nil, err
	}
	attachedDatasets := make([]string, 0)
	for i := range datasets.Items {
		dataset := &datasets.Items[i]
		if datasetReferencesSecurityPolicy(dataset, policy.Name) {
			attachedDatasets = append(attachedDatasets, dataset.Name)
		}
	}
	sort.Strings(missingTargets)
	sort.Strings(attachedDatasets)
	return missingTargets, attachedDatasets, nil
}

func (r *SecurityPolicyReconciler) requestsForDataset(ctx context.Context, obj client.Object) []reconcile.Request {
	var policies fusekiv1alpha1.SecurityPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range policies.Items {
		policy := &policies.Items[i]
		if !securityPolicyTargetsDataset(policy, obj.GetName()) && !datasetReferencesSecurityPolicy(obj.(*fusekiv1alpha1.Dataset), policy.Name) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	}
	return requests
}

func (r *SecurityPolicyReconciler) securityPolicyReadyMessage(policy *fusekiv1alpha1.SecurityPolicy, attachedDatasets []string) string {
	targetCount := len(securityPolicyTargetDatasetNames(policy))
	if len(attachedDatasets) == 0 {
		return fmt.Sprintf("Resolved %d target dataset(s); policy is not yet attached to any Dataset.", targetCount)
	}
	return fmt.Sprintf("Resolved %d target dataset(s); referenced by %d Dataset(s): %s.", targetCount, len(attachedDatasets), strings.Join(attachedDatasets, ", "))
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=shaclpolicies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=shaclpolicies/status,verbs=get;update;patch
func (r *SHACLPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy fusekiv1alpha1.SHACLPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	updated := policy.DeepCopy()
	updated.Status.ObservedGeneration = policy.Generation
	updated.Status.Phase = "Pending"
	issues := validateSHACLPolicySpec(&policy)
	apimeta.SetStatusCondition(&updated.Status.Conditions, scaffoldedCondition(policy.Generation, "SHACLPolicy", issues))
	if len(issues) > 0 {
		updated.Status.Phase = "Invalid"
	}

	if !reflect.DeepEqual(policy.Status, updated.Status) {
		policy.Status = updated.Status
		if err := r.Status().Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *SHACLPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.SHACLPolicy{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=importrequests,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=importrequests/status,verbs=get;update;patch
func (r *ImportRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var request fusekiv1alpha1.ImportRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	updated := request.DeepCopy()
	updated.Status.ObservedGeneration = request.Generation
	updated.Status.Phase = "Pending"
	updated.Status.JobName = request.JobName()
	issues := validateImportRequestSpec(&request)
	apimeta.SetStatusCondition(&updated.Status.Conditions, scaffoldedCondition(request.Generation, "ImportRequest", issues))
	if len(issues) > 0 {
		updated.Status.Phase = "Invalid"
	}

	if !reflect.DeepEqual(request.Status, updated.Status) {
		request.Status = updated.Status
		if err := r.Status().Update(ctx, &request); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ImportRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ImportRequest{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=exportrequests,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=exportrequests/status,verbs=get;update;patch
func (r *ExportRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var request fusekiv1alpha1.ExportRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	updated := request.DeepCopy()
	updated.Status.ObservedGeneration = request.Generation
	updated.Status.Phase = "Pending"
	updated.Status.JobName = request.JobName()
	issues := validateExportRequestSpec(&request)
	apimeta.SetStatusCondition(&updated.Status.Conditions, scaffoldedCondition(request.Generation, "ExportRequest", issues))
	if len(issues) > 0 {
		updated.Status.Phase = "Invalid"
	}

	if !reflect.DeepEqual(request.Status, updated.Status) {
		request.Status = updated.Status
		if err := r.Status().Update(ctx, &request); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ExportRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ExportRequest{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=ingestpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=ingestpipelines/status,verbs=get;update;patch
func (r *IngestPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pipeline fusekiv1alpha1.IngestPipeline
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	updated := pipeline.DeepCopy()
	updated.Status.ObservedGeneration = pipeline.Generation
	updated.Status.Phase = "Pending"
	issues := validateIngestPipelineSpec(&pipeline)
	apimeta.SetStatusCondition(&updated.Status.Conditions, scaffoldedCondition(pipeline.Generation, "IngestPipeline", issues))
	if len(issues) > 0 {
		updated.Status.Phase = "Invalid"
	}

	if !reflect.DeepEqual(pipeline.Status, updated.Status) {
		pipeline.Status = updated.Status
		if err := r.Status().Update(ctx, &pipeline); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *IngestPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.IngestPipeline{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=changesubscriptions,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=changesubscriptions/status,verbs=get;update;patch
func (r *ChangeSubscriptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var subscription fusekiv1alpha1.ChangeSubscription
	if err := r.Get(ctx, req.NamespacedName, &subscription); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	updated := subscription.DeepCopy()
	updated.Status.ObservedGeneration = subscription.Generation
	updated.Status.Phase = "Pending"
	issues := validateChangeSubscriptionSpec(&subscription)
	apimeta.SetStatusCondition(&updated.Status.Conditions, scaffoldedCondition(subscription.Generation, "ChangeSubscription", issues))
	if len(issues) > 0 {
		updated.Status.Phase = "Invalid"
	}

	if !reflect.DeepEqual(subscription.Status, updated.Status) {
		subscription.Status = updated.Status
		if err := r.Status().Update(ctx, &subscription); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ChangeSubscriptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ChangeSubscription{}).
		Complete(r)
}

func scaffoldedCondition(generation int64, resource string, issues []string) metav1.Condition {
	condition := metav1.Condition{
		Type:               configuredConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             "Scaffolded",
		Message:            fmt.Sprintf("%s controller scaffolded for v0.2.0 M0; implementation pending.", resource),
		ObservedGeneration: generation,
	}
	if len(issues) > 0 {
		condition.Reason = "InvalidSpec"
		condition.Message = joinValidationIssues(issues)
	}
	return condition
}
