package controller

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

const restoreReadyConditionType = "RestoreReady"

func resolveActiveRestoreRequest(ctx context.Context, c client.Client, namespace, serverName string) (*fusekiv1alpha1.RestoreRequest, error) {
	var requests fusekiv1alpha1.RestoreRequestList
	if err := c.List(ctx, &requests, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	active := make([]fusekiv1alpha1.RestoreRequest, 0)
	for i := range requests.Items {
		request := requests.Items[i]
		if request.Spec.TargetRef.Kind != fusekiv1alpha1.RestoreRequestTargetKindRDFDeltaServer || request.Spec.TargetRef.Name != serverName {
			continue
		}
		if request.Status.Phase == "Succeeded" || request.Status.Phase == "Failed" {
			continue
		}
		active = append(active, request)
	}

	if len(active) == 0 {
		return nil, nil
	}

	sort.Slice(active, func(i, j int) bool {
		if active[i].CreationTimestamp.Equal(&active[j].CreationTimestamp) {
			return active[i].Name < active[j].Name
		}
		return active[i].CreationTimestamp.Before(&active[j].CreationTimestamp)
	})

	return &active[0], nil
}

func restoreStatefulSetWorkloadCondition(activeRestore *fusekiv1alpha1.RestoreRequest, readyReplicas, desiredReplicas int32) (metav1.ConditionStatus, string, string) {
	if activeRestore != nil {
		return metav1.ConditionFalse, "RestoreInProgress", fmt.Sprintf("RestoreRequest %q is in progress; RDF Delta is scaled down.", activeRestore.Name)
	}

	return workloadConditionStatus(readyReplicas, desiredReplicas), workloadConditionReason(readyReplicas, desiredReplicas), workloadConditionMessage("RDF Delta", readyReplicas, desiredReplicas)
}
