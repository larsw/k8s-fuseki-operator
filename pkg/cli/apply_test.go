package cli

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

func TestApplyDatasetCreatesWhenMissing(t *testing.T) {
	kubeClient := fakeclient.NewClientBuilder().WithScheme(kubeScheme).Build()
	dataset := buildDataset(datasetParams{
		Namespace:    "default",
		ResourceName: "example-dataset",
		Type:         "TDB2",
	})

	action, err := applyDataset(context.Background(), kubeClient, dataset)
	if err != nil {
		t.Fatalf("apply dataset: %v", err)
	}
	if action != "created" {
		t.Fatalf("unexpected action: %q", action)
	}

	stored := &fusekiv1alpha1.Dataset{}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: dataset.Namespace, Name: dataset.Name}, stored); err != nil {
		t.Fatalf("get dataset: %v", err)
	}
	if stored.Spec.Name != "example-dataset" {
		t.Fatalf("unexpected spec name: %q", stored.Spec.Name)
	}
}

func TestApplyFusekiClusterUpdatesExistingSpec(t *testing.T) {
	existing := buildFusekiCluster(fusekiClusterParams{
		Namespace:      "default",
		ResourceName:   "example",
		Image:          "ghcr.io/larsw/k8s-fuseki-operator/fuseki:old",
		Replicas:       1,
		HTTPPort:       3030,
		RDFDeltaServer: "old-delta",
	})
	kubeClient := fakeclient.NewClientBuilder().WithScheme(kubeScheme).WithObjects(existing).Build()

	desired := buildFusekiCluster(fusekiClusterParams{
		Namespace:      "default",
		ResourceName:   "example",
		Image:          "ghcr.io/larsw/k8s-fuseki-operator/fuseki:new",
		Replicas:       3,
		HTTPPort:       4040,
		RDFDeltaServer: "example-delta",
		Datasets:       []string{"primary"},
	})

	action, err := applyFusekiCluster(context.Background(), kubeClient, desired)
	if err != nil {
		t.Fatalf("apply fuseki cluster: %v", err)
	}
	if action != "configured" {
		t.Fatalf("unexpected action: %q", action)
	}

	stored := &fusekiv1alpha1.FusekiCluster{}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: desired.Namespace, Name: desired.Name}, stored); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if stored.Spec.Image != "ghcr.io/larsw/k8s-fuseki-operator/fuseki:new" {
		t.Fatalf("unexpected image: %q", stored.Spec.Image)
	}
	if stored.Spec.RDFDeltaServerRef.Name != "example-delta" {
		t.Fatalf("unexpected rdf delta ref: %q", stored.Spec.RDFDeltaServerRef.Name)
	}
	if len(stored.Spec.DatasetRefs) != 1 || stored.Spec.DatasetRefs[0].Name != "primary" {
		t.Fatalf("unexpected dataset refs: %#v", stored.Spec.DatasetRefs)
	}
}

func TestApplyRestoreRequestUpdatesExistingSpec(t *testing.T) {
	existing := buildRestoreRequest(restoreRequestParams{
		Namespace:    "default",
		Name:         "example-restore",
		TargetName:   "old-delta",
		BackupObject: "old-object",
	})
	kubeClient := fakeclient.NewClientBuilder().WithScheme(kubeScheme).WithObjects(existing).Build()

	desired := buildRestoreRequest(restoreRequestParams{
		Namespace:    "default",
		Name:         "example-restore",
		TargetName:   "example-delta",
		BackupObject: "20260308T120000Z-example-delta",
		BackupPolicy: "nightly",
	})

	action, err := applyRestoreRequest(context.Background(), kubeClient, desired)
	if err != nil {
		t.Fatalf("apply restore request: %v", err)
	}
	if action != "configured" {
		t.Fatalf("unexpected action: %q", action)
	}

	stored := &fusekiv1alpha1.RestoreRequest{}
	if err := kubeClient.Get(context.Background(), client.ObjectKey{Namespace: desired.Namespace, Name: desired.Name}, stored); err != nil {
		t.Fatalf("get restore request: %v", err)
	}
	if stored.Spec.TargetRef.Name != "example-delta" {
		t.Fatalf("unexpected target: %q", stored.Spec.TargetRef.Name)
	}
	if stored.Spec.BackupPolicyRef == nil || stored.Spec.BackupPolicyRef.Name != "nightly" {
		t.Fatalf("expected backup policy reference to be set")
	}
}
