package cli

import (
	"strings"
	"testing"
)

func TestRenderStatusTextIncludesKeySections(t *testing.T) {
	summary := StatusSummary{
		Operator: OperatorStatusSummary{
			Namespace:       DefaultOperatorNamespace,
			DeploymentName:  OperatorDeploymentName,
			Phase:           "Available",
			ReadyReplicas:   1,
			DesiredReplicas: 1,
			Image:           "ghcr.io/larsw/k8s-fuseki-operator/controller:dev",
		},
		FusekiClusters: []FusekiClusterStatusSummary{{
			Namespace:        "default",
			Name:             "example",
			Phase:            "Ready",
			ReadyReplicas:    3,
			DesiredReplicas:  3,
			ActiveWritePod:   "example-0",
			ReadServiceName:  "example-read",
			WriteServiceName: "example-write",
		}},
	}

	output, err := renderStatusText(summary)
	if err != nil {
		t.Fatalf("render status text: %v", err)
	}

	checks := []string{
		"Operator",
		"FusekiClusters",
		"fuseki-system",
		"example-write",
		"RDFDeltaServers",
		"(none)",
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Fatalf("expected output to contain %q, got:\n%s", check, output)
		}
	}
}
