package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
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
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
func (r *SHACLPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy fusekiv1alpha1.SHACLPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	specIssues := validateSHACLPolicySpec(&policy)
	missingRefs, invalidRefs, err := r.shaclSourceReferenceIssues(ctx, &policy)
	if err != nil {
		return ctrl.Result{}, err
	}

	updated := policy.DeepCopy()
	updated.Status.ObservedGeneration = policy.Generation
	updated.Status.Phase = "Ready"
	conditionStatus := metav1.ConditionTrue
	conditionReason := "SourcesResolved"
	conditionMessage := fmt.Sprintf("Resolved %d SHACL source(s).", len(policy.Spec.Sources))
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
			conditionReason = "SourcesMissing"
			conditionMessage = "Waiting for SHACL sources: " + strings.Join(missingRefs, ", ")
		case len(missingRefs) == 0 && len(invalidRefs) > 0:
			conditionReason = "SourcesInvalid"
			conditionMessage = "Waiting for SHACL sources to contain usable content: " + strings.Join(invalidRefs, ", ")
		default:
			conditionReason = "SourcesUnresolved"
			conditionMessage = "Waiting for SHACL sources and usable content: " + strings.Join(append(missingRefs, invalidRefs...), ", ")
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

	if len(specIssues) > 0 {
		return ctrl.Result{}, nil
	}
	if len(missingRefs) > 0 || len(invalidRefs) > 0 {
		return ctrl.Result{RequeueAfter: securityProfileRequeueInterval}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SHACLPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.SHACLPolicy{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSHACLConfigMap)).
		Complete(r)
}

func (r *SHACLPolicyReconciler) shaclSourceReferenceIssues(ctx context.Context, policy *fusekiv1alpha1.SHACLPolicy) ([]string, []string, error) {
	missingRefs := make([]string, 0)
	invalidRefs := make([]string, 0)
	for _, source := range policy.Spec.Sources {
		if source.Type != fusekiv1alpha1.SHACLSourceTypeConfigMap || source.ConfigMapRef == nil || source.ConfigMapRef.Name == "" {
			continue
		}

		refLabel := fmt.Sprintf("ConfigMap/%s[%s]", source.ConfigMapRef.Name, source.Key)
		var configMap corev1.ConfigMap
		if err := r.Get(ctx, client.ObjectKey{Namespace: policy.Namespace, Name: source.ConfigMapRef.Name}, &configMap); err != nil {
			if apierrors.IsNotFound(err) {
				missingRefs = append(missingRefs, refLabel)
				continue
			}
			return nil, nil, err
		}

		value, ok := configMap.Data[source.Key]
		if !ok {
			invalidRefs = append(invalidRefs, refLabel+" missing key")
			continue
		}
		if strings.TrimSpace(value) == "" {
			invalidRefs = append(invalidRefs, refLabel+" empty")
		}
	}

	sort.Strings(missingRefs)
	sort.Strings(invalidRefs)
	return missingRefs, invalidRefs, nil
}

func (r *SHACLPolicyReconciler) requestsForSHACLConfigMap(ctx context.Context, obj client.Object) []reconcile.Request {
	var policies fusekiv1alpha1.SHACLPolicyList
	if err := r.List(ctx, &policies, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range policies.Items {
		policy := &policies.Items[i]
		for _, source := range policy.Spec.Sources {
			if source.ConfigMapRef == nil || source.ConfigMapRef.Name != obj.GetName() {
				continue
			}
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
			break
		}
	}
	return requests
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=importrequests,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=importrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets;fusekiservers;fusekiclusters;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
func (r *ImportRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var request fusekiv1alpha1.ImportRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if transferRequestPhaseTerminal(request.Status.Phase) && request.Status.ObservedGeneration >= request.Generation {
		return ctrl.Result{}, nil
	}

	target, err := resolveTransferTarget(ctx, r.Client, request.Namespace, request.Spec.Target, transferDirectionImport)
	if err != nil {
		return ctrl.Result{}, err
	}

	configuredStatus := target.Status
	configuredReason := target.Reason
	configuredMessage := target.Message
	importStatus := metav1.ConditionFalse
	importReason := "ImportPending"
	importMessage := "Waiting for import prerequisites."
	phase := "Pending"
	jobName := request.JobName()

	if configuredStatus == metav1.ConditionTrue {
		secretStatus, err := resolveTransferSecretDependency(ctx, r.Client, request.Namespace, request.Spec.Source.SecretRef, "spec.source.secretRef", requiredSourceSecretKeys(request.Spec.Source))
		if err != nil {
			return ctrl.Result{}, err
		}
		configuredStatus = secretStatus.Status
		configuredReason = secretStatus.Reason
		configuredMessage = secretStatus.Message
		if configuredStatus == metav1.ConditionTrue {
			job, err := reconcileImportJob(ctx, r.Client, r.Scheme, &request, target)
			if err != nil {
				return ctrl.Result{}, err
			}
			phase, importStatus, importReason, importMessage = transferJobProgress(job, jobName, "Import")
		}
	}

	updated := request.DeepCopy()
	updated.Status.ObservedGeneration = request.Generation
	updated.Status.Phase = phase
	updated.Status.JobName = jobName
	if configuredStatus != metav1.ConditionTrue {
		updated.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             configuredStatus,
		Reason:             configuredReason,
		Message:            configuredMessage,
		ObservedGeneration: request.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               importCompletedConditionType,
		Status:             importStatus,
		Reason:             importReason,
		Message:            importMessage,
		ObservedGeneration: request.Generation,
	})

	if !reflect.DeepEqual(request.Status, updated.Status) {
		request.Status = updated.Status
		if err := r.Status().Update(ctx, &request); err != nil {
			return ctrl.Result{}, err
		}
	}
	if transferRequestPhaseTerminal(updated.Status.Phase) {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
}

func (r *ImportRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ImportRequest{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=exportrequests,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=exportrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=datasets;fusekiservers;fusekiclusters;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
func (r *ExportRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var request fusekiv1alpha1.ExportRequest
	if err := r.Get(ctx, req.NamespacedName, &request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if transferRequestPhaseTerminal(request.Status.Phase) && request.Status.ObservedGeneration >= request.Generation {
		return ctrl.Result{}, nil
	}

	target, err := resolveTransferTarget(ctx, r.Client, request.Namespace, request.Spec.Target, transferDirectionExport)
	if err != nil {
		return ctrl.Result{}, err
	}

	configuredStatus := target.Status
	configuredReason := target.Reason
	configuredMessage := target.Message
	exportStatus := metav1.ConditionFalse
	exportReason := "ExportPending"
	exportMessage := "Waiting for export prerequisites."
	phase := "Pending"
	jobName := request.JobName()
	artifactRef := resolveExportArtifactRef(&request)

	if configuredStatus == metav1.ConditionTrue {
		secretStatus, err := resolveTransferSecretDependency(ctx, r.Client, request.Namespace, request.Spec.Sink.SecretRef, "spec.sink.secretRef", requiredSinkSecretKeys(request.Spec.Sink))
		if err != nil {
			return ctrl.Result{}, err
		}
		configuredStatus = secretStatus.Status
		configuredReason = secretStatus.Reason
		configuredMessage = secretStatus.Message
		if configuredStatus == metav1.ConditionTrue {
			job, resolvedArtifactRef, err := reconcileExportJob(ctx, r.Client, r.Scheme, &request, target)
			if err != nil {
				return ctrl.Result{}, err
			}
			artifactRef = resolvedArtifactRef
			phase, exportStatus, exportReason, exportMessage = transferJobProgress(job, jobName, "Export")
		}
	}

	updated := request.DeepCopy()
	updated.Status.ObservedGeneration = request.Generation
	updated.Status.Phase = phase
	updated.Status.JobName = jobName
	updated.Status.ArtifactRef = artifactRef
	if configuredStatus != metav1.ConditionTrue {
		updated.Status.Phase = "Pending"
	}
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             configuredStatus,
		Reason:             configuredReason,
		Message:            configuredMessage,
		ObservedGeneration: request.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               exportCompletedConditionType,
		Status:             exportStatus,
		Reason:             exportReason,
		Message:            exportMessage,
		ObservedGeneration: request.Generation,
	})

	if !reflect.DeepEqual(request.Status, updated.Status) {
		request.Status = updated.Status
		if err := r.Status().Update(ctx, &request); err != nil {
			return ctrl.Result{}, err
		}
	}
	if transferRequestPhaseTerminal(updated.Status.Phase) {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
}

func (r *ExportRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ExportRequest{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=ingestpipelines,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=ingestpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=shaclpolicies;datasets;fusekiservers;fusekiclusters;securityprofiles,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs;cronjobs,verbs=get;list;watch;create;update;patch;delete
func (r *IngestPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pipeline fusekiv1alpha1.IngestPipeline
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pipeline.Spec.Schedule == "" && transferRequestPhaseTerminal(pipeline.Status.Phase) && pipeline.Status.ObservedGeneration >= pipeline.Generation {
		return ctrl.Result{}, nil
	}

	specIssues := validateIngestPipelineSpec(&pipeline)
	configuredStatus := metav1.ConditionTrue
	configuredReason := "DependenciesResolved"
	configuredMessage := fmt.Sprintf("IngestPipeline %q dependencies are resolved.", pipeline.Name)
	executionStatus := metav1.ConditionFalse
	executionReason := "IngestPending"
	executionMessage := "Waiting for ingest prerequisites."
	phase := "Pending"
	var lastRunTime *metav1.Time
	targetKind := "Job"
	targetName := ingestPipelineJobName(&pipeline)
	shaclPolicyName := ""
	failureAction := ""
	if pipeline.Spec.SHACLPolicyRef != nil {
		shaclPolicyName = pipeline.Spec.SHACLPolicyRef.Name
	}

	if len(specIssues) > 0 {
		phase = "Invalid"
		configuredStatus = metav1.ConditionFalse
		configuredReason = "InvalidSpec"
		configuredMessage = joinValidationIssues(specIssues)
	} else {
		shaclStatus, err := resolveSHACLPolicyDependency(ctx, r.Client, pipeline.Namespace, pipeline.Spec.SHACLPolicyRef)
		if err != nil {
			return ctrl.Result{}, err
		}
		if shaclStatus.Status != metav1.ConditionTrue {
			phase = "Pending"
			configuredStatus = metav1.ConditionFalse
			configuredReason = shaclStatus.Reason
			configuredMessage = shaclStatus.Message
		} else {
			if shaclStatus.Policy != nil {
				failureAction = string(shaclStatus.Policy.DesiredFailureAction())
			}
			target, err := resolveTransferTarget(ctx, r.Client, pipeline.Namespace, pipeline.Spec.Target, transferDirectionImport)
			if err != nil {
				return ctrl.Result{}, err
			}
			if target.Status != metav1.ConditionTrue {
				phase = "Pending"
				configuredStatus = metav1.ConditionFalse
				configuredReason = target.Reason
				configuredMessage = target.Message
			} else {
				secretStatus, err := resolveTransferSecretDependency(ctx, r.Client, pipeline.Namespace, pipeline.Spec.Source.SecretRef, "spec.source.secretRef", requiredSourceSecretKeys(pipeline.Spec.Source))
				if err != nil {
					return ctrl.Result{}, err
				}
				if secretStatus.Status != metav1.ConditionTrue {
					phase = "Pending"
					configuredStatus = metav1.ConditionFalse
					configuredReason = secretStatus.Reason
					configuredMessage = secretStatus.Message
				} else if pipeline.Spec.Schedule != "" {
					targetKind = "CronJob"
					targetName = ingestPipelineCronJobName(&pipeline)
					cronJob, err := reconcileIngestCronJob(ctx, r.Client, r.Scheme, &pipeline, target, shaclStatus.Policy)
					if err != nil {
						return ctrl.Result{}, err
					}
					phase, lastRunTime, executionStatus, executionReason, executionMessage = ingestCronJobProgress(cronJob, pipeline.Name, pipeline.Spec.Suspend)
					configuredReason = "PipelineScheduled"
					if pipeline.Spec.Suspend {
						configuredMessage = fmt.Sprintf("IngestPipeline CronJob %q is reconciled and suspended.", cronJob.Name)
					} else {
						configuredMessage = fmt.Sprintf("IngestPipeline CronJob %q is reconciled.", cronJob.Name)
					}
					if err := deleteIngestJob(ctx, r.Client, pipeline.Namespace, ingestPipelineJobName(&pipeline)); err != nil {
						return ctrl.Result{}, err
					}
				} else if pipeline.Spec.Suspend {
					phase = "Suspended"
					executionReason = "IngestSuspended"
					executionMessage = fmt.Sprintf("IngestPipeline %q is suspended.", pipeline.Name)
					configuredReason = "PipelineSuspended"
					configuredMessage = fmt.Sprintf("IngestPipeline %q dependencies are resolved but execution is suspended.", pipeline.Name)
					if err := deleteIngestCronJob(ctx, r.Client, pipeline.Namespace, ingestPipelineCronJobName(&pipeline)); err != nil {
						return ctrl.Result{}, err
					}
				} else {
					job, err := reconcileIngestJob(ctx, r.Client, r.Scheme, &pipeline, target, shaclStatus.Policy)
					if err != nil {
						return ctrl.Result{}, err
					}
					if job == nil {
						return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
					}
					phase, lastRunTime, executionStatus, executionReason, executionMessage = ingestJobProgress(job, ingestPipelineJobName(&pipeline))
					configuredReason = "PipelineJobReady"
					configuredMessage = fmt.Sprintf("IngestPipeline Job %q is reconciled.", job.Name)
					if err := deleteIngestCronJob(ctx, r.Client, pipeline.Namespace, ingestPipelineCronJobName(&pipeline)); err != nil {
						return ctrl.Result{}, err
					}
				}
			}
		}
	}

	if len(specIssues) > 0 || configuredStatus != metav1.ConditionTrue {
		if err := deleteIngestCronJob(ctx, r.Client, pipeline.Namespace, ingestPipelineCronJobName(&pipeline)); err != nil {
			return ctrl.Result{}, err
		}
	}

	updated := pipeline.DeepCopy()
	updated.Status.ObservedGeneration = pipeline.Generation
	updated.Status.Phase = phase
	updated.Status.LastRunTime = lastRunTime
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             configuredStatus,
		Reason:             configuredReason,
		Message:            configuredMessage,
		ObservedGeneration: pipeline.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               ingestCompletedConditionType,
		Status:             executionStatus,
		Reason:             executionReason,
		Message:            executionMessage,
		ObservedGeneration: pipeline.Generation,
	})

	if !reflect.DeepEqual(pipeline.Status, updated.Status) {
		pipeline.Status = updated.Status
		if err := r.Status().Update(ctx, &pipeline); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := reconcileIngestPipelineSummary(ctx, r.Client, r.Scheme, &pipeline, configuredStatus, configuredReason, configuredMessage, executionStatus, executionReason, executionMessage, targetKind, targetName, shaclPolicyName, failureAction, lastRunTime); err != nil {
		return ctrl.Result{}, err
	}

	if len(specIssues) > 0 || phase == "Succeeded" || phase == "Failed" || phase == "Suspended" {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
}

func (r *IngestPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.IngestPipeline{}).
		Watches(&fusekiv1alpha1.SHACLPolicy{}, handler.EnqueueRequestsFromMapFunc(r.requestsForSHACLPolicy)).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

func (r *IngestPipelineReconciler) requestsForSHACLPolicy(ctx context.Context, obj client.Object) []reconcile.Request {
	var pipelines fusekiv1alpha1.IngestPipelineList
	if err := r.List(ctx, &pipelines, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range pipelines.Items {
		pipeline := &pipelines.Items[i]
		if pipeline.Spec.SHACLPolicyRef == nil || pipeline.Spec.SHACLPolicyRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	}
	return requests
}

type shaclPolicyDependencyStatus struct {
	Policy  *fusekiv1alpha1.SHACLPolicy
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

func resolveSHACLPolicyDependency(ctx context.Context, c client.Client, namespace string, ref *corev1.LocalObjectReference) (shaclPolicyDependencyStatus, error) {
	if ref == nil || ref.Name == "" {
		return shaclPolicyDependencyStatus{
			Status:  metav1.ConditionFalse,
			Reason:  "SHACLPolicyNotConfigured",
			Message: "Waiting for SHACLPolicy reference.",
		}, nil
	}

	var policy fusekiv1alpha1.SHACLPolicy
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			return shaclPolicyDependencyStatus{
				Status:  metav1.ConditionFalse,
				Reason:  "SHACLPolicyNotFound",
				Message: fmt.Sprintf("Waiting for SHACLPolicy %q.", ref.Name),
			}, nil
		}
		return shaclPolicyDependencyStatus{}, err
	}

	configuredCondition := apimeta.FindStatusCondition(policy.Status.Conditions, configuredConditionType)
	if configuredCondition == nil || configuredCondition.Status != metav1.ConditionTrue {
		message := fmt.Sprintf("Waiting for SHACLPolicy %q to resolve its sources.", policy.Name)
		if configuredCondition != nil && configuredCondition.Message != "" {
			message = configuredCondition.Message
		}
		return shaclPolicyDependencyStatus{
			Policy:  &policy,
			Status:  metav1.ConditionFalse,
			Reason:  "SHACLPolicyNotReady",
			Message: message,
		}, nil
	}

	return shaclPolicyDependencyStatus{
		Policy:  &policy,
		Status:  metav1.ConditionTrue,
		Reason:  "SHACLPolicyReady",
		Message: fmt.Sprintf("SHACLPolicy %q is ready.", policy.Name),
	}, nil
}

// +kubebuilder:rbac:groups=fuseki.apache.org,resources=changesubscriptions,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=changesubscriptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fuseki.apache.org,resources=rdfdeltaservers;datasets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
func (r *ChangeSubscriptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var subscription fusekiv1alpha1.ChangeSubscription
	if err := r.Get(ctx, req.NamespacedName, &subscription); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	specIssues := validateChangeSubscriptionSpec(&subscription)
	configuredStatus := metav1.ConditionTrue
	configuredReason := "DependenciesResolved"
	configuredMessage := fmt.Sprintf("ChangeSubscription %q dependencies are resolved.", subscription.Name)
	deliveryStatus := metav1.ConditionTrue
	deliveryReason := "SubscriptionCurrent"
	deliveryMessage := fmt.Sprintf("ChangeSubscription %q is up to date.", subscription.Name)
	phase := "Ready"
	lastCheckpoint := subscription.Status.LastCheckpoint
	checkpoint, checkpointErr := parseSubscriptionCheckpoint(subscription.Status.LastCheckpoint)
	logName := changeSubscriptionLogName(&subscription)
	deliveryJobName := changeSubscriptionJobName(&subscription)
	pendingRange := ""
	artifactRef := ""
	var lag *int
	var currentVersionValue *int

	if len(specIssues) > 0 {
		phase = "Invalid"
		configuredStatus = metav1.ConditionFalse
		configuredReason = "InvalidSpec"
		configuredMessage = joinValidationIssues(specIssues)
		deliveryStatus = metav1.ConditionFalse
		deliveryReason = "SubscriptionInvalid"
		deliveryMessage = "ChangeSubscription has an invalid specification."
	} else if checkpointErr != nil {
		phase = "Failed"
		deliveryStatus = metav1.ConditionFalse
		deliveryReason = "InvalidCheckpoint"
		deliveryMessage = checkpointErr.Error()
	} else {
		var server fusekiv1alpha1.RDFDeltaServer
		if err := r.Get(ctx, client.ObjectKey{Namespace: subscription.Namespace, Name: subscription.Spec.RDFDeltaServerRef.Name}, &server); err != nil {
			if apierrors.IsNotFound(err) {
				phase = "Pending"
				configuredStatus = metav1.ConditionFalse
				configuredReason = "RDFDeltaServerNotFound"
				configuredMessage = fmt.Sprintf("Waiting for RDFDeltaServer %q.", subscription.Spec.RDFDeltaServerRef.Name)
				deliveryStatus = metav1.ConditionFalse
				deliveryReason = "SubscriptionPending"
				deliveryMessage = configuredMessage
			} else {
				return ctrl.Result{}, err
			}
		} else if subscription.Spec.Target != nil {
			var dataset fusekiv1alpha1.Dataset
			if err := r.Get(ctx, client.ObjectKey{Namespace: subscription.Namespace, Name: subscription.Spec.Target.DatasetRef.Name}, &dataset); err != nil {
				if apierrors.IsNotFound(err) {
					phase = "Pending"
					configuredStatus = metav1.ConditionFalse
					configuredReason = "TargetDatasetNotFound"
					configuredMessage = fmt.Sprintf("Waiting for Dataset %q.", subscription.Spec.Target.DatasetRef.Name)
					deliveryStatus = metav1.ConditionFalse
					deliveryReason = "SubscriptionPending"
					deliveryMessage = configuredMessage
				} else {
					return ctrl.Result{}, err
				}
			}
		}

		if configuredStatus == metav1.ConditionTrue {
			secretStatus, err := resolveTransferSecretDependency(ctx, r.Client, subscription.Namespace, subscription.Spec.Sink.SecretRef, "spec.sink.secretRef", requiredSinkSecretKeys(subscription.Spec.Sink))
			if err != nil {
				return ctrl.Result{}, err
			}
			if secretStatus.Status != metav1.ConditionTrue {
				phase = "Pending"
				configuredStatus = metav1.ConditionFalse
				configuredReason = secretStatus.Reason
				configuredMessage = secretStatus.Message
				deliveryStatus = metav1.ConditionFalse
				deliveryReason = "SubscriptionPending"
				deliveryMessage = configuredMessage
			} else if subscription.Spec.Suspend {
				phase = "Suspended"
				configuredReason = "SubscriptionSuspended"
				configuredMessage = fmt.Sprintf("ChangeSubscription %q is suspended.", subscription.Name)
				deliveryStatus = metav1.ConditionFalse
				deliveryReason = "SubscriptionSuspended"
				deliveryMessage = configuredMessage
				if err := deleteChangeSubscriptionJob(ctx, r.Client, subscription.Namespace, changeSubscriptionJobName(&subscription)); err != nil {
					return ctrl.Result{}, err
				}
			} else {
				job := &batchv1.Job{}
				jobErr := r.Get(ctx, client.ObjectKey{Namespace: subscription.Namespace, Name: changeSubscriptionJobName(&subscription)}, job)
				switch {
				case jobErr == nil:
					artifactRef = job.Annotations[subscriptionArtifactRefAnnotation]
					pendingRange = job.Annotations[subscriptionStartCheckpointAnnotation] + "-" + job.Annotations[subscriptionEndCheckpointAnnotation]
					if endCheckpoint, err := strconv.Atoi(job.Annotations[subscriptionEndCheckpointAnnotation]); err == nil {
						currentVersionValue = &endCheckpoint
						lagValue := endCheckpoint - checkpoint
						if lagValue < 0 {
							lagValue = 0
						}
						lag = &lagValue
					}
					if job.Annotations[subscriptionGenerationAnnotation] != strconv.FormatInt(subscription.Generation, 10) {
						if err := deleteChangeSubscriptionJob(ctx, r.Client, subscription.Namespace, job.Name); err != nil {
							return ctrl.Result{}, err
						}
						return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
					}
					var completedCheckpoint string
					var progressStatus metav1.ConditionStatus
					var progressReason, progressMessage string
					phase, completedCheckpoint, progressStatus, progressReason, progressMessage = subscriptionJobProgress(job, subscription.Name)
					deliveryStatus = progressStatus
					deliveryReason = progressReason
					deliveryMessage = progressMessage
					if completedCheckpoint != "" {
						lastCheckpoint = completedCheckpoint
						if err := deleteChangeSubscriptionJob(ctx, r.Client, subscription.Namespace, job.Name); err != nil {
							return ctrl.Result{}, err
						}
					} else if phase == "Failed" {
						if err := deleteChangeSubscriptionJob(ctx, r.Client, subscription.Namespace, job.Name); err != nil {
							return ctrl.Result{}, err
						}
					}
				case apierrors.IsNotFound(jobErr):
					currentVersion, err := rdfDeltaLogVersionFetcher(ctx, changeSubscriptionServerURL(&server), changeSubscriptionLogName(&subscription))
					if err != nil {
						phase = "Pending"
						deliveryStatus = metav1.ConditionFalse
						deliveryReason = "SubscriptionSourceUnreachable"
						deliveryMessage = fmt.Sprintf("Unable to query RDF Delta checkpoint for %q: %v", server.Name, err)
						break
					}
					currentVersionValue = &currentVersion
					lagValue := currentVersion - checkpoint
					if lagValue < 0 {
						lagValue = 0
					}
					lag = &lagValue
					if currentVersion <= checkpoint {
						phase = "Ready"
						deliveryStatus = metav1.ConditionTrue
						deliveryReason = "SubscriptionCurrent"
						deliveryMessage = fmt.Sprintf("ChangeSubscription %q is current at checkpoint %d.", subscription.Name, checkpoint)
						break
					}
					job, reconciledArtifactRef, err := reconcileChangeSubscriptionJob(ctx, r.Client, r.Scheme, &subscription, &server, checkpoint+1, currentVersion)
					if err != nil {
						return ctrl.Result{}, err
					}
					if job == nil {
						return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
					}
					artifactRef = reconciledArtifactRef
					pendingRange = fmt.Sprintf("%d-%d", checkpoint+1, currentVersion)
					phase = "Running"
					deliveryStatus = metav1.ConditionFalse
					deliveryReason = "SubscriptionLagging"
					deliveryMessage = fmt.Sprintf("ChangeSubscription %q is %d checkpoint(s) behind and is delivering checkpoints %d-%d to %s.", subscription.Name, lagValue, checkpoint+1, currentVersion, artifactRef)
				default:
					return ctrl.Result{}, jobErr
				}
			}
		}
	}

	updated := subscription.DeepCopy()
	updated.Status.ObservedGeneration = subscription.Generation
	updated.Status.Phase = phase
	updated.Status.LastCheckpoint = lastCheckpoint
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               configuredConditionType,
		Status:             configuredStatus,
		Reason:             configuredReason,
		Message:            configuredMessage,
		ObservedGeneration: subscription.Generation,
	})
	apimeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
		Type:               subscriptionDeliveredConditionType,
		Status:             deliveryStatus,
		Reason:             deliveryReason,
		Message:            deliveryMessage,
		ObservedGeneration: subscription.Generation,
	})

	if !reflect.DeepEqual(subscription.Status, updated.Status) {
		subscription.Status = updated.Status
		if err := r.Status().Update(ctx, &subscription); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := reconcileChangeSubscriptionSummary(ctx, r.Client, r.Scheme, &subscription, configuredStatus, configuredReason, configuredMessage, deliveryStatus, deliveryReason, deliveryMessage, logName, deliveryJobName, pendingRange, artifactRef, lag, currentVersionValue); err != nil {
		return ctrl.Result{}, err
	}

	if len(specIssues) > 0 || checkpointErr != nil || phase == "Suspended" {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: transferRequestRequeueInterval}, nil
}

func (r *ChangeSubscriptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fusekiv1alpha1.ChangeSubscription{}).
		Watches(&fusekiv1alpha1.RDFDeltaServer{}, handler.EnqueueRequestsFromMapFunc(r.requestsForRDFDeltaServer)).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func (r *ChangeSubscriptionReconciler) requestsForRDFDeltaServer(ctx context.Context, obj client.Object) []reconcile.Request {
	var subscriptions fusekiv1alpha1.ChangeSubscriptionList
	if err := r.List(ctx, &subscriptions, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range subscriptions.Items {
		subscription := &subscriptions.Items[i]
		if subscription.Spec.RDFDeltaServerRef.Name != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(subscription)})
	}
	return requests
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
