package controller

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func ingestPipelineSummaryConfigMapName(pipeline *fusekiv1alpha1.IngestPipeline) string {
	return pipeline.Name + "-ingest-summary"
}

func changeSubscriptionSummaryConfigMapName(subscription *fusekiv1alpha1.ChangeSubscription) string {
	return subscription.Name + "-subscription-summary"
}

func reconcileOwnedSummaryConfigMap(ctx context.Context, c client.Client, scheme *runtime.Scheme, owner client.Object, name string, labels map[string]string, data map[string]string) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: owner.GetNamespace()}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, configMap, func() error {
		configMap.Labels = labels
		configMap.Data = data
		return controllerutil.SetControllerReference(owner, configMap, scheme)
	})
	return err
}

func conditionStatusString(status metav1.ConditionStatus) string {
	return string(status)
}

func timeString(value *metav1.Time) string {
	if value == nil {
		return ""
	}
	return value.Time.UTC().Format("2006-01-02T15:04:05Z")
}

func summaryLabels(name, instance, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       name,
		"app.kubernetes.io/instance":   instance,
		"app.kubernetes.io/managed-by": "fuseki-operator",
		"fuseki.apache.org/component":  component,
	}
}

func reconcileIngestPipelineSummary(ctx context.Context, c client.Client, scheme *runtime.Scheme, pipeline *fusekiv1alpha1.IngestPipeline, configuredStatus metav1.ConditionStatus, configuredReason, configuredMessage string, executionStatus metav1.ConditionStatus, executionReason, executionMessage, targetKind, targetName, shaclPolicyName, failureAction string, lastRunTime *metav1.Time) error {
	data := map[string]string{
		"phase":             pipeline.Status.Phase,
		"configuredStatus":  conditionStatusString(configuredStatus),
		"configuredReason":  configuredReason,
		"configuredMessage": configuredMessage,
		"executionStatus":   conditionStatusString(executionStatus),
		"executionReason":   executionReason,
		"executionMessage":  executionMessage,
		"lastRunTime":       timeString(lastRunTime),
		"targetKind":        targetKind,
		"targetName":        targetName,
		"reportDirectory":   ingestReportDirectory,
		"schedule":          pipeline.Spec.Schedule,
		"sourceType":        string(pipeline.Spec.Source.Type),
		"shaclPolicy":       shaclPolicyName,
		"failureAction":     failureAction,
	}
	labels := summaryLabels("fuseki-ingest-summary", pipeline.Name, "ingest-summary")
	return reconcileOwnedSummaryConfigMap(ctx, c, scheme, pipeline, ingestPipelineSummaryConfigMapName(pipeline), labels, data)
}

func reconcileChangeSubscriptionSummary(ctx context.Context, c client.Client, scheme *runtime.Scheme, subscription *fusekiv1alpha1.ChangeSubscription, configuredStatus metav1.ConditionStatus, configuredReason, configuredMessage string, deliveryStatus metav1.ConditionStatus, deliveryReason, deliveryMessage, logName, jobName, pendingRange, artifactRef string, lag *int, currentVersion *int) error {
	data := map[string]string{
		"phase":             subscription.Status.Phase,
		"configuredStatus":  conditionStatusString(configuredStatus),
		"configuredReason":  configuredReason,
		"configuredMessage": configuredMessage,
		"deliveryStatus":    conditionStatusString(deliveryStatus),
		"deliveryReason":    deliveryReason,
		"deliveryMessage":   deliveryMessage,
		"lastCheckpoint":    subscription.Status.LastCheckpoint,
		"logName":           logName,
		"deliveryJob":       jobName,
		"pendingRange":      pendingRange,
		"artifactRef":       artifactRef,
	}
	if lag != nil {
		data["lag"] = strconv.Itoa(*lag)
	}
	if currentVersion != nil {
		data["currentVersion"] = strconv.Itoa(*currentVersion)
	}
	labels := summaryLabels("rdf-delta-subscription-summary", subscription.Name, "change-subscription-summary")
	return reconcileOwnedSummaryConfigMap(ctx, c, scheme, subscription, changeSubscriptionSummaryConfigMapName(subscription), labels, data)
}
