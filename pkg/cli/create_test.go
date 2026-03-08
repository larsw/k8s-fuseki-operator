package cli

import "testing"

func TestBuildDatasetDefaultsSpecNameToResourceName(t *testing.T) {
	resource := buildDataset(datasetParams{
		Namespace:    "default",
		ResourceName: "example-dataset",
		Type:         "TDB2",
		Spatial:      true,
		BackupPolicy: "nightly",
	})

	if resource.Name != "example-dataset" {
		t.Fatalf("unexpected resource name: %q", resource.Name)
	}
	if resource.Spec.Name != "example-dataset" {
		t.Fatalf("unexpected spec name: %q", resource.Spec.Name)
	}
	if resource.Spec.BackupPolicyRef == nil || resource.Spec.BackupPolicyRef.Name != "nightly" {
		t.Fatalf("expected backup policy ref to be set")
	}
	if resource.Spec.Spatial == nil || !resource.Spec.Spatial.Enabled {
		t.Fatalf("expected spatial defaults to be enabled")
	}
}

func TestBuildRDFDeltaServerSetsRequestedFields(t *testing.T) {
	resource := buildRDFDeltaServer(rdfDeltaServerParams{
		Namespace:    "default",
		ResourceName: "example-delta",
		Image:        "ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:latest",
		Replicas:     2,
		ServicePort:  1066,
		BackupPolicy: "nightly",
	})

	if resource.Spec.Image != "ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:latest" {
		t.Fatalf("unexpected image: %q", resource.Spec.Image)
	}
	if resource.Spec.Replicas != 2 {
		t.Fatalf("unexpected replicas: %d", resource.Spec.Replicas)
	}
	if resource.Spec.BackupPolicyRef == nil || resource.Spec.BackupPolicyRef.Name != "nightly" {
		t.Fatalf("expected backup policy ref to be set")
	}
}

func TestBuildFusekiClusterSetsReferences(t *testing.T) {
	resource := buildFusekiCluster(fusekiClusterParams{
		Namespace:      "default",
		ResourceName:   "example",
		Image:          "ghcr.io/larsw/k8s-fuseki-operator/fuseki:6.0.0",
		Replicas:       3,
		HTTPPort:       3030,
		RDFDeltaServer: "example-delta",
		Datasets:       []string{"primary", "secondary"},
	})

	if resource.Spec.RDFDeltaServerRef.Name != "example-delta" {
		t.Fatalf("unexpected rdf delta reference: %q", resource.Spec.RDFDeltaServerRef.Name)
	}
	if len(resource.Spec.DatasetRefs) != 2 {
		t.Fatalf("expected 2 dataset refs, got %d", len(resource.Spec.DatasetRefs))
	}
	if resource.Spec.DatasetRefs[1].Name != "secondary" {
		t.Fatalf("unexpected dataset ref: %q", resource.Spec.DatasetRefs[1].Name)
	}
}
