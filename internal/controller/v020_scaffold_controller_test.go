package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestSecurityProfileReconcileInvalidLocalModeWithRangerSettings(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	profile := &fusekiv1alpha1.SecurityProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-local", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityProfileSpec{
			Authorization: &fusekiv1alpha1.SecurityAuthorizationSpec{
				Mode: fusekiv1alpha1.AuthorizationModeLocal,
				Ranger: &fusekiv1alpha1.RangerAuthorizationSpec{
					AdminURL:    "https://ranger.example.com",
					ServiceName: "fuseki-default",
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityProfile{}).
		WithObjects(profile).
		Build()

	reconciler := &SecurityProfileReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(profile)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for invalid static spec, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.SecurityProfile{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(profile), updated); err != nil {
		t.Fatalf("get updated profile: %v", err)
	}
	if updated.Status.Phase != "Invalid" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "InvalidSpec" {
		t.Fatalf("expected InvalidSpec condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "authorization.ranger is not allowed") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestIngestPipelineReconcileInvalidSpec(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Source: fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeFilesystem, URI: "https://example.com/data.ttl"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Invalid" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "InvalidSpec" {
		t.Fatalf("expected InvalidSpec condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "spec.target.datasetRef.name is required") || !strings.Contains(condition.Message, "spec.shaclPolicyRef.name is required") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestSHACLPolicyReconcilePendingWhenConfigMapSourceMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: "default"},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:         fusekiv1alpha1.SHACLSourceTypeConfigMap,
				ConfigMapRef: &corev1.LocalObjectReference{Name: "missing-shapes"},
				Key:          "shapes.ttl",
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SHACLPolicy{}).
		WithObjects(policy).
		Build()

	reconciler := &SHACLPolicyReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("expected requeue for missing configmap source")
	}

	updated := &fusekiv1alpha1.SHACLPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "SourcesMissing" {
		t.Fatalf("expected SourcesMissing condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "missing-shapes") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestIngestPipelineReconcileCreatesOneShotJobWhenDependenciesResolved(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	shaclPolicy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: "default"},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
		},
		Status: fusekiv1alpha1.SHACLPolicyStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: "SourcesResolved"}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: shaclPolicy.Name},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(dataset, server, shaclPolicy, pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s for active ingest pipeline, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "PipelineJobReady" {
		t.Fatalf("expected DependenciesResolved condition, got %#v", condition)
	}
	executionCondition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if executionCondition == nil || executionCondition.Reason != "IngestPending" {
		t.Fatalf("expected IngestPending execution condition, got %#v", executionCondition)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: pipeline.Namespace, Name: ingestPipelineJobName(pipeline)}, job); err != nil {
		t.Fatalf("get ingest job: %v", err)
	}
	if got := job.Annotations[ingestReportDirectoryAnnotation]; got != ingestReportDirectory {
		t.Fatalf("unexpected ingest report directory annotation: %q", got)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "FUSEKI_IMPORT_URL"); got != "http://example-server:3030/primary/data" {
		t.Fatalf("unexpected ingest URL: %q", got)
	}
	if got := envVarValue(container.Env, "SHACL_SOURCE_COUNT"); got != "1" {
		t.Fatalf("unexpected SHACL source count: %q", got)
	}
	if !strings.Contains(container.Command[2], "shacl_bin") || !strings.Contains(container.Command[2], "validate --shapes") {
		t.Fatalf("expected ingest script to invoke SHACL validation, got %q", container.Command[2])
	}
	summary := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: pipeline.Namespace, Name: ingestPipelineSummaryConfigMapName(pipeline)}, summary); err != nil {
		t.Fatalf("get ingest summary configmap: %v", err)
	}
	if got := summary.Data["reportDirectory"]; got != ingestReportDirectory {
		t.Fatalf("unexpected ingest summary report directory: %q", got)
	}
	if got := summary.Data["executionReason"]; got != "IngestPending" {
		t.Fatalf("unexpected ingest summary execution reason: %q", got)
	}
}

func TestIngestPipelineReconcileSurfacesFailedJobSummary(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	shaclPolicy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: "default"},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
			FailureAction: fusekiv1alpha1.SHACLFailureActionReportOnly,
		},
		Status: fusekiv1alpha1.SHACLPolicyStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: "SourcesResolved"}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: shaclPolicy.Name},
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingestPipelineJobName(pipeline),
			Namespace: pipeline.Namespace,
			Annotations: map[string]string{
				ingestGenerationAnnotation:      strconv.FormatInt(pipeline.Generation, 10),
				ingestReportDirectoryAnnotation: ingestReportDirectory,
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(dataset, server, shaclPolicy, pipeline, job).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for failed ingest pipeline, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Failed" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	executionCondition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if executionCondition == nil || executionCondition.Reason != "IngestFailed" {
		t.Fatalf("expected IngestFailed execution condition, got %#v", executionCondition)
	}
	if !strings.Contains(executionCondition.Message, ingestReportDirectory) {
		t.Fatalf("expected ingest failure message to mention report directory, got %q", executionCondition.Message)
	}

	summary := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: pipeline.Namespace, Name: ingestPipelineSummaryConfigMapName(pipeline)}, summary); err != nil {
		t.Fatalf("get ingest summary configmap: %v", err)
	}
	if got := summary.Data["phase"]; got != "Failed" {
		t.Fatalf("unexpected ingest summary phase: %q", got)
	}
	if got := summary.Data["executionReason"]; got != "IngestFailed" {
		t.Fatalf("unexpected ingest summary execution reason: %q", got)
	}
	if got := summary.Data["failureAction"]; got != string(fusekiv1alpha1.SHACLFailureActionReportOnly) {
		t.Fatalf("unexpected ingest summary failure action: %q", got)
	}
	if got := summary.Data["shaclPolicy"]; got != shaclPolicy.Name {
		t.Fatalf("unexpected ingest summary SHACL policy: %q", got)
	}
	if got := summary.Data["reportDirectory"]; got != ingestReportDirectory {
		t.Fatalf("unexpected ingest summary report directory: %q", got)
	}
}

func TestIngestPipelineReconcilePendingWhenSHACLPolicyNotReady(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	shaclPolicy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: "default"},
		Status: fusekiv1alpha1.SHACLPolicyStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionFalse, Reason: "SourcesMissing", Message: "Waiting for SHACL sources."}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: shaclPolicy.Name},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(dataset, server, shaclPolicy, pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("expected requeue for unresolved SHACL policy")
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "SHACLPolicyNotReady" {
		t.Fatalf("expected SHACLPolicyNotReady condition, got %#v", condition)
	}
}

func TestIngestPipelineReconcileCreatesCronJobWhenScheduled(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	server := &fusekiv1alpha1.FusekiServer{ObjectMeta: metav1.ObjectMeta{Name: "example-server", Namespace: "default"}, Spec: fusekiv1alpha1.FusekiServerSpec{Image: "ghcr.io/example/fuseki:6.0.0", DatasetRefs: []corev1.LocalObjectReference{{Name: dataset.Name}}}}
	shaclPolicy := &fusekiv1alpha1.SHACLPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-shacl", Namespace: "default"},
		Spec: fusekiv1alpha1.SHACLPolicySpec{
			Sources: []fusekiv1alpha1.SHACLSource{{
				Type:   fusekiv1alpha1.SHACLSourceTypeInline,
				Inline: "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:NodeShape .",
			}},
		},
		Status: fusekiv1alpha1.SHACLPolicyStatus{
			Conditions: []metav1.Condition{{Type: configuredConditionType, Status: metav1.ConditionTrue, Reason: "SourcesResolved"}},
		},
	}
	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "example-pipeline", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: shaclPolicy.Name},
			Schedule:       "*/15 * * * *",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(dataset, server, shaclPolicy, pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("expected requeue for scheduled pipeline status refresh")
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "PipelineScheduled" {
		t.Fatalf("expected PipelineScheduled condition, got %#v", condition)
	}
	executionCondition := apimeta.FindStatusCondition(updated.Status.Conditions, ingestCompletedConditionType)
	if executionCondition == nil || executionCondition.Reason != "IngestScheduled" {
		t.Fatalf("expected IngestScheduled execution condition, got %#v", executionCondition)
	}

	cronJob := &batchv1.CronJob{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: pipeline.Namespace, Name: ingestPipelineCronJobName(pipeline)}, cronJob); err != nil {
		t.Fatalf("get ingest cronjob: %v", err)
	}
	if cronJob.Spec.Schedule != pipeline.Spec.Schedule {
		t.Fatalf("unexpected ingest cronjob schedule: %q", cronJob.Spec.Schedule)
	}
	if cronJob.Spec.Suspend == nil || *cronJob.Spec.Suspend {
		t.Fatalf("expected ingest cronjob to be active, got %#v", cronJob.Spec.Suspend)
	}
}

func TestIngestPipelineReconcileInvalidWhenScheduleMalformed(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	pipeline := &fusekiv1alpha1.IngestPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-schedule", Namespace: "default"},
		Spec: fusekiv1alpha1.IngestPipelineSpec{
			Target:         fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "example-dataset"}},
			Source:         fusekiv1alpha1.DataSourceSpec{Type: fusekiv1alpha1.DataSourceTypeURL, URI: "https://example.com/data.ttl"},
			SHACLPolicyRef: &corev1.LocalObjectReference{Name: "example-shacl"},
			Schedule:       "not-a-cron",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.IngestPipeline{}).
		WithObjects(pipeline).
		Build()

	reconciler := &IngestPipelineReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pipeline)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.IngestPipeline{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pipeline), updated); err != nil {
		t.Fatalf("get updated pipeline: %v", err)
	}
	if updated.Status.Phase != "Invalid" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "InvalidSpec" || !strings.Contains(condition.Message, "spec.schedule must be a valid standard cron expression") {
		t.Fatalf("expected invalid schedule condition, got %#v", condition)
	}
}

func TestChangeSubscriptionReconcileSuspendedWhenDependenciesResolved(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	server := &fusekiv1alpha1.RDFDeltaServer{ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"}}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: "default"},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/subscription.nq"},
			Suspend:           true,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ChangeSubscription{}).
		WithObjects(server, dataset, subscription).
		Build()

	reconciler := &ChangeSubscriptionReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(subscription)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for suspended subscription, got %s", result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(subscription), updated); err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if updated.Status.Phase != "Suspended" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "SubscriptionSuspended" {
		t.Fatalf("expected SubscriptionSuspended condition, got %#v", condition)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionSuspended" {
		t.Fatalf("expected suspended delivery condition, got %#v", deliveryCondition)
	}
}

func TestChangeSubscriptionReconcileCreatesDeliveryJobWhenCheckpointAdvances(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	previousFetcher := rdfDeltaLogVersionFetcher
	rdfDeltaLogVersionFetcher = func(context.Context, string, string) (int, error) {
		return 7, nil
	}
	t.Cleanup(func() { rdfDeltaLogVersionFetcher = previousFetcher })

	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec:       fusekiv1alpha1.RDFDeltaServerSpec{Image: "ghcr.io/example/rdf-delta:latest"},
	}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: "default"},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/"},
		},
		Status: fusekiv1alpha1.ChangeSubscriptionStatus{LastCheckpoint: "5"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ChangeSubscription{}).
		WithObjects(server, dataset, subscription).
		Build()

	reconciler := &ChangeSubscriptionReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(subscription)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(subscription), updated); err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "5" {
		t.Fatalf("unexpected checkpoint: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionLagging" {
		t.Fatalf("expected pending delivery condition, got %#v", deliveryCondition)
	}

	job := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: changeSubscriptionJobName(subscription)}, job); err != nil {
		t.Fatalf("get subscription job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := envVarValue(container.Env, "RDF_DELTA_BASE_URL"); got != "http://delta:1066" {
		t.Fatalf("unexpected RDF Delta URL: %q", got)
	}
	if got := envVarValue(container.Env, "RDF_DELTA_LOG_NAME"); got != dataset.Name {
		t.Fatalf("unexpected log name: %q", got)
	}
	if got := envVarValue(container.Env, "SUBSCRIPTION_START_VERSION"); got != "6" {
		t.Fatalf("unexpected start version: %q", got)
	}
	if got := envVarValue(container.Env, "SUBSCRIPTION_END_VERSION"); got != "7" {
		t.Fatalf("unexpected end version: %q", got)
	}
	expectedArtifact := fmt.Sprintf("/exports/%s-%012d-%012d.rdfpatch", subscription.Name, 6, 7)
	if got := envVarValue(container.Env, "TRANSFER_ARTIFACT_REF"); got != expectedArtifact {
		t.Fatalf("unexpected artifact ref: %q", got)
	}
	if !strings.Contains(container.Command[2], "/patch/${version}") {
		t.Fatalf("expected subscription script to fetch patch versions, got %q", container.Command[2])
	}
	summary := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: changeSubscriptionSummaryConfigMapName(subscription)}, summary); err != nil {
		t.Fatalf("get subscription summary configmap: %v", err)
	}
	if got := summary.Data["artifactRef"]; got != expectedArtifact {
		t.Fatalf("unexpected subscription summary artifact ref: %q", got)
	}
	if got := summary.Data["lag"]; got != "2" {
		t.Fatalf("unexpected subscription lag: %q", got)
	}
	if got := summary.Data["pendingRange"]; got != "6-7" {
		t.Fatalf("unexpected subscription pending range: %q", got)
	}
}

func TestChangeSubscriptionReconcileAdvancesCheckpointWhenDeliveryJobCompletes(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec:       fusekiv1alpha1.RDFDeltaServerSpec{Image: "ghcr.io/example/rdf-delta:latest"},
	}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: "default"},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/"},
		},
		Status: fusekiv1alpha1.ChangeSubscriptionStatus{LastCheckpoint: "5"},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      changeSubscriptionJobName(subscription),
			Namespace: subscription.Namespace,
			Annotations: map[string]string{
				subscriptionGenerationAnnotation:      strconv.FormatInt(subscription.Generation, 10),
				subscriptionStartCheckpointAnnotation: "6",
				subscriptionEndCheckpointAnnotation:   "7",
				subscriptionArtifactRefAnnotation:     "/exports/example-subscription-000000000006-000000000007.rdfpatch",
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ChangeSubscription{}).
		WithObjects(server, dataset, subscription, job).
		Build()

	reconciler := &ChangeSubscriptionReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(subscription)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(subscription), updated); err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "7" {
		t.Fatalf("unexpected checkpoint: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionDelivered" {
		t.Fatalf("expected delivered condition, got %#v", deliveryCondition)
	}
	summary := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: changeSubscriptionSummaryConfigMapName(subscription)}, summary); err != nil {
		t.Fatalf("get subscription summary configmap: %v", err)
	}
	if got := summary.Data["lastCheckpoint"]; got != "7" {
		t.Fatalf("unexpected subscription summary checkpoint: %q", got)
	}
	if got := summary.Data["artifactRef"]; got != "/exports/example-subscription-000000000006-000000000007.rdfpatch" {
		t.Fatalf("unexpected subscription summary artifact ref: %q", got)
	}

	deletedJob := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: job.Name}, deletedJob); !apierrors.IsNotFound(err) {
		t.Fatalf("expected delivery job to be deleted after completion, got err=%v", err)
	}
}

func TestChangeSubscriptionReconcileSurfacesFailedDeliverySummary(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	server := &fusekiv1alpha1.RDFDeltaServer{
		ObjectMeta: metav1.ObjectMeta{Name: "delta", Namespace: "default"},
		Spec:       fusekiv1alpha1.RDFDeltaServerSpec{Image: "ghcr.io/example/rdf-delta:latest"},
	}
	dataset := &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"}, Spec: fusekiv1alpha1.DatasetSpec{Name: "primary"}}
	subscription := &fusekiv1alpha1.ChangeSubscription{
		ObjectMeta: metav1.ObjectMeta{Name: "example-subscription", Namespace: "default"},
		Spec: fusekiv1alpha1.ChangeSubscriptionSpec{
			RDFDeltaServerRef: corev1.LocalObjectReference{Name: server.Name},
			Target:            &fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: dataset.Name}},
			Sink:              fusekiv1alpha1.DataSinkSpec{Type: fusekiv1alpha1.DataSinkTypeFilesystem, Path: "/exports/"},
		},
		Status: fusekiv1alpha1.ChangeSubscriptionStatus{LastCheckpoint: "5"},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      changeSubscriptionJobName(subscription),
			Namespace: subscription.Namespace,
			Annotations: map[string]string{
				subscriptionGenerationAnnotation:      strconv.FormatInt(subscription.Generation, 10),
				subscriptionStartCheckpointAnnotation: "6",
				subscriptionEndCheckpointAnnotation:   "7",
				subscriptionArtifactRefAnnotation:     "/exports/example-subscription-000000000006-000000000007.rdfpatch",
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.ChangeSubscription{}).
		WithObjects(server, dataset, subscription, job).
		Build()

	reconciler := &ChangeSubscriptionReconciler{Client: k8sClient, Scheme: scheme}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(subscription)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter != transferRequestRequeueInterval {
		t.Fatalf("expected requeue interval %s, got %s", transferRequestRequeueInterval, result.RequeueAfter)
	}

	updated := &fusekiv1alpha1.ChangeSubscription{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(subscription), updated); err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if updated.Status.Phase != "Failed" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	if updated.Status.LastCheckpoint != "5" {
		t.Fatalf("unexpected checkpoint: %q", updated.Status.LastCheckpoint)
	}
	deliveryCondition := apimeta.FindStatusCondition(updated.Status.Conditions, subscriptionDeliveredConditionType)
	if deliveryCondition == nil || deliveryCondition.Reason != "SubscriptionFailed" {
		t.Fatalf("expected SubscriptionFailed condition, got %#v", deliveryCondition)
	}
	if !strings.Contains(deliveryCondition.Message, "/exports/example-subscription-000000000006-000000000007.rdfpatch") {
		t.Fatalf("expected failed delivery message to mention artifact, got %q", deliveryCondition.Message)
	}

	summary := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: changeSubscriptionSummaryConfigMapName(subscription)}, summary); err != nil {
		t.Fatalf("get subscription summary configmap: %v", err)
	}
	if got := summary.Data["phase"]; got != "Failed" {
		t.Fatalf("unexpected subscription summary phase: %q", got)
	}
	if got := summary.Data["deliveryReason"]; got != "SubscriptionFailed" {
		t.Fatalf("unexpected subscription summary delivery reason: %q", got)
	}
	if got := summary.Data["artifactRef"]; got != "/exports/example-subscription-000000000006-000000000007.rdfpatch" {
		t.Fatalf("unexpected subscription summary artifact ref: %q", got)
	}
	if got := summary.Data["pendingRange"]; got != "6-7" {
		t.Fatalf("unexpected subscription summary pending range: %q", got)
	}
	if got := summary.Data["lag"]; got != "2" {
		t.Fatalf("unexpected subscription summary lag: %q", got)
	}
	if got := summary.Data["currentVersion"]; got != "7" {
		t.Fatalf("unexpected subscription summary current version: %q", got)
	}

	deletedJob := &batchv1.Job{}
	if err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: subscription.Namespace, Name: job.Name}, deletedJob); !apierrors.IsNotFound(err) {
		t.Fatalf("expected failed delivery job to be deleted, got err=%v", err)
	}
}

func TestSecurityPolicyReconcileReadyWhenTargetsResolved(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.SecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-policy", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityPolicySpec{
			Rules: []fusekiv1alpha1.SecurityPolicyRule{{
				Target:     fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "example-dataset"}},
				Actions:    []fusekiv1alpha1.SecurityPolicyAction{fusekiv1alpha1.SecurityPolicyActionQuery},
				Subjects:   []fusekiv1alpha1.SecuritySubject{{Type: fusekiv1alpha1.SecuritySubjectTypeUser, Value: "alice"}},
				Expression: "PUBLIC",
			}},
		},
	}
	dataset := &fusekiv1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "example-dataset", Namespace: "default"},
		Spec: fusekiv1alpha1.DatasetSpec{
			Name:             "primary",
			SecurityPolicies: []corev1.LocalObjectReference{{Name: "example-policy"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityPolicy{}).
		WithObjects(policy, dataset).
		Build()

	reconciler := &SecurityPolicyReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.SecurityPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Ready" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "DependenciesResolved" {
		t.Fatalf("expected DependenciesResolved condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "referenced by 1 Dataset") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}

func TestSecurityPolicyReconcilePendingWhenTargetDatasetMissing(t *testing.T) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := fusekiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add fuseki scheme: %v", err)
	}

	policy := &fusekiv1alpha1.SecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "example-policy", Namespace: "default"},
		Spec: fusekiv1alpha1.SecurityPolicySpec{
			Rules: []fusekiv1alpha1.SecurityPolicyRule{{
				Target:     fusekiv1alpha1.DatasetAccessTarget{DatasetRef: corev1.LocalObjectReference{Name: "missing-dataset"}},
				Actions:    []fusekiv1alpha1.SecurityPolicyAction{fusekiv1alpha1.SecurityPolicyActionQuery},
				Subjects:   []fusekiv1alpha1.SecuritySubject{{Type: fusekiv1alpha1.SecuritySubjectTypeUser, Value: "alice"}},
				Expression: "PUBLIC",
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&fusekiv1alpha1.SecurityPolicy{}).
		WithObjects(policy).
		Build()

	reconciler := &SecurityPolicyReconciler{Client: k8sClient, Scheme: scheme}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(policy)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	updated := &fusekiv1alpha1.SecurityPolicy{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(policy), updated); err != nil {
		t.Fatalf("get updated policy: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Fatalf("unexpected phase: %q", updated.Status.Phase)
	}
	condition := apimeta.FindStatusCondition(updated.Status.Conditions, configuredConditionType)
	if condition == nil || condition.Reason != "DependenciesMissing" {
		t.Fatalf("expected DependenciesMissing condition, got %#v", condition)
	}
	if !strings.Contains(condition.Message, "missing-dataset") {
		t.Fatalf("unexpected condition message: %q", condition.Message)
	}
}
