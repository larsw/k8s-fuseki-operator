package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type statusOptions struct {
	root              *RootOptions
	Namespace         string
	OperatorNamespace string
	Output            string
}

type StatusSummary struct {
	Operator        OperatorStatusSummary         `json:"operator"`
	FusekiClusters  []FusekiClusterStatusSummary  `json:"fusekiClusters"`
	RDFDeltaServers []RDFDeltaStatusSummary       `json:"rdfDeltaServers"`
	Datasets        []DatasetStatusSummary        `json:"datasets"`
	Endpoints       []EndpointStatusSummary       `json:"endpoints"`
	BackupPolicies  []BackupPolicyStatusSummary   `json:"backupPolicies"`
	RestoreRequests []RestoreRequestStatusSummary `json:"restoreRequests"`
}

type OperatorStatusSummary struct {
	Namespace         string `json:"namespace"`
	DeploymentName    string `json:"deploymentName"`
	Phase             string `json:"phase"`
	ReadyReplicas     int32  `json:"readyReplicas"`
	DesiredReplicas   int32  `json:"desiredReplicas"`
	AvailableReplicas int32  `json:"availableReplicas"`
	Image             string `json:"image,omitempty"`
}

type FusekiClusterStatusSummary struct {
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	Phase            string `json:"phase"`
	ReadyReplicas    int32  `json:"readyReplicas"`
	DesiredReplicas  int32  `json:"desiredReplicas"`
	ActiveWritePod   string `json:"activeWritePod,omitempty"`
	ReadServiceName  string `json:"readServiceName,omitempty"`
	WriteServiceName string `json:"writeServiceName,omitempty"`
}

type RDFDeltaStatusSummary struct {
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	Phase             string `json:"phase"`
	ReadyReplicas     int32  `json:"readyReplicas"`
	DesiredReplicas   int32  `json:"desiredReplicas"`
	ServiceName       string `json:"serviceName,omitempty"`
	BackupCronJobName string `json:"backupCronJobName,omitempty"`
	ActiveRestoreName string `json:"activeRestoreName,omitempty"`
}

type DatasetStatusSummary struct {
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	Phase         string `json:"phase"`
	ConfigMapName string `json:"configMapName,omitempty"`
}

type EndpointStatusSummary struct {
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	Phase            string `json:"phase"`
	ReadServiceName  string `json:"readServiceName,omitempty"`
	WriteServiceName string `json:"writeServiceName,omitempty"`
}

type BackupPolicyStatusSummary struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Phase     string `json:"phase"`
	Schedule  string `json:"schedule"`
	Bucket    string `json:"bucket"`
}

type RestoreRequestStatusSummary struct {
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	Phase             string `json:"phase"`
	TargetName        string `json:"targetName,omitempty"`
	ResolvedBackupRef string `json:"resolvedBackupRef,omitempty"`
	JobName           string `json:"jobName,omitempty"`
}

func newStatusCommand(root *RootOptions) *cobra.Command {
	options := &statusOptions{
		root:              root,
		OperatorNamespace: DefaultOperatorNamespace,
		Output:            "text",
	}

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show operator and custom resource status",
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}

			summary, err := gatherStatusSummary(cmd.Context(), kubeClient, options.OperatorNamespace, options.Namespace)
			if err != nil {
				return err
			}

			switch options.Output {
			case "text":
				output, err := renderStatusText(summary)
				if err != nil {
					return err
				}
				_, err = fmt.Fprint(cmd.OutOrStdout(), output)
				return err
			case "json":
				encoded, err := json.MarshalIndent(summary, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
				return err
			default:
				return fmt.Errorf("unsupported output format %q", options.Output)
			}
		},
	}

	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace to inspect. Empty means all namespaces.")
	cmd.Flags().StringVar(&options.OperatorNamespace, "operator-namespace", options.OperatorNamespace, "Namespace of the operator deployment.")
	cmd.Flags().StringVarP(&options.Output, "output", "o", options.Output, "Output format: text or json.")

	return cmd
}

func gatherStatusSummary(ctx context.Context, kubeClient client.Client, operatorNamespace, namespace string) (StatusSummary, error) {
	summary := StatusSummary{
		Operator: OperatorStatusSummary{
			Namespace:      operatorNamespace,
			DeploymentName: OperatorDeploymentName,
			Phase:          "NotInstalled",
		},
	}

	operator := &appsv1.Deployment{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: OperatorDeploymentName}, operator); err != nil {
		if !apierrors.IsNotFound(err) {
			return StatusSummary{}, err
		}
	} else {
		summary.Operator = summarizeOperatorDeployment(operator)
	}

	listOptions := listNamespaceOptions(namespace)

	var clusters fusekiv1alpha1.FusekiClusterList
	if err := kubeClient.List(ctx, &clusters, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range clusters.Items {
		summary.FusekiClusters = append(summary.FusekiClusters, FusekiClusterStatusSummary{
			Namespace:        item.Namespace,
			Name:             item.Name,
			Phase:            defaultPhase(item.Status.Phase),
			ReadyReplicas:    item.Status.ReadyReplicas,
			DesiredReplicas:  item.DesiredReplicas(),
			ActiveWritePod:   item.Status.ActiveWritePod,
			ReadServiceName:  item.Status.ReadServiceName,
			WriteServiceName: item.Status.WriteServiceName,
		})
	}

	var deltas fusekiv1alpha1.RDFDeltaServerList
	if err := kubeClient.List(ctx, &deltas, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range deltas.Items {
		summary.RDFDeltaServers = append(summary.RDFDeltaServers, RDFDeltaStatusSummary{
			Namespace:         item.Namespace,
			Name:              item.Name,
			Phase:             defaultPhase(item.Status.Phase),
			ReadyReplicas:     item.Status.ReadyReplicas,
			DesiredReplicas:   item.DesiredReplicas(),
			ServiceName:       item.Status.ServiceName,
			BackupCronJobName: item.Status.BackupCronJobName,
			ActiveRestoreName: item.Status.ActiveRestoreName,
		})
	}

	var datasets fusekiv1alpha1.DatasetList
	if err := kubeClient.List(ctx, &datasets, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range datasets.Items {
		summary.Datasets = append(summary.Datasets, DatasetStatusSummary{
			Namespace:     item.Namespace,
			Name:          item.Name,
			Phase:         defaultPhase(item.Status.Phase),
			ConfigMapName: item.Status.ConfigMapName,
		})
	}

	var endpoints fusekiv1alpha1.EndpointList
	if err := kubeClient.List(ctx, &endpoints, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range endpoints.Items {
		summary.Endpoints = append(summary.Endpoints, EndpointStatusSummary{
			Namespace:        item.Namespace,
			Name:             item.Name,
			Phase:            defaultPhase(item.Status.Phase),
			ReadServiceName:  item.Status.ReadServiceName,
			WriteServiceName: item.Status.WriteServiceName,
		})
	}

	var policies fusekiv1alpha1.BackupPolicyList
	if err := kubeClient.List(ctx, &policies, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range policies.Items {
		summary.BackupPolicies = append(summary.BackupPolicies, BackupPolicyStatusSummary{
			Namespace: item.Namespace,
			Name:      item.Name,
			Phase:     defaultPhase(item.Status.Phase),
			Schedule:  item.Spec.Schedule,
			Bucket:    item.Spec.S3.Bucket,
		})
	}

	var restores fusekiv1alpha1.RestoreRequestList
	if err := kubeClient.List(ctx, &restores, listOptions...); err != nil {
		return StatusSummary{}, err
	}
	for _, item := range restores.Items {
		summary.RestoreRequests = append(summary.RestoreRequests, RestoreRequestStatusSummary{
			Namespace:         item.Namespace,
			Name:              item.Name,
			Phase:             defaultPhase(item.Status.Phase),
			TargetName:        item.Status.TargetName,
			ResolvedBackupRef: item.Status.ResolvedBackupRef,
			JobName:           item.Status.JobName,
		})
	}

	return summary, nil
}

func summarizeOperatorDeployment(deployment *appsv1.Deployment) OperatorStatusSummary {
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	phase := "Progressing"
	if deployment.Status.AvailableReplicas >= desiredReplicas && deployment.Status.ObservedGeneration >= deployment.Generation {
		phase = "Available"
	}

	image := ""
	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		image = deployment.Spec.Template.Spec.Containers[0].Image
	}

	return OperatorStatusSummary{
		Namespace:         deployment.Namespace,
		DeploymentName:    deployment.Name,
		Phase:             phase,
		ReadyReplicas:     deployment.Status.ReadyReplicas,
		DesiredReplicas:   desiredReplicas,
		AvailableReplicas: deployment.Status.AvailableReplicas,
		Image:             image,
	}
}

func renderStatusText(summary StatusSummary) (string, error) {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)

	printSection(writer, "Operator", []string{"NAMESPACE", "NAME", "PHASE", "READY", "IMAGE"}, [][]string{{
		summary.Operator.Namespace,
		summary.Operator.DeploymentName,
		summary.Operator.Phase,
		fmt.Sprintf("%d/%d", summary.Operator.ReadyReplicas, summary.Operator.DesiredReplicas),
		summary.Operator.Image,
	}})
	printSection(writer, "FusekiClusters", []string{"NAMESPACE", "NAME", "PHASE", "READY", "WRITE_POD", "READ_SERVICE", "WRITE_SERVICE"}, clusterRows(summary.FusekiClusters))
	printSection(writer, "RDFDeltaServers", []string{"NAMESPACE", "NAME", "PHASE", "READY", "SERVICE", "BACKUP_CRONJOB", "ACTIVE_RESTORE"}, deltaRows(summary.RDFDeltaServers))
	printSection(writer, "Datasets", []string{"NAMESPACE", "NAME", "PHASE", "CONFIG_MAP"}, datasetRows(summary.Datasets))
	printSection(writer, "Endpoints", []string{"NAMESPACE", "NAME", "PHASE", "READ_SERVICE", "WRITE_SERVICE"}, endpointRows(summary.Endpoints))
	printSection(writer, "BackupPolicies", []string{"NAMESPACE", "NAME", "PHASE", "SCHEDULE", "BUCKET"}, backupPolicyRows(summary.BackupPolicies))
	printSection(writer, "RestoreRequests", []string{"NAMESPACE", "NAME", "PHASE", "TARGET", "BACKUP", "JOB"}, restoreRows(summary.RestoreRequests))

	if err := writer.Flush(); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func listNamespaceOptions(namespace string) []client.ListOption {
	if namespace == "" {
		return nil
	}
	return []client.ListOption{client.InNamespace(namespace)}
}

func defaultPhase(phase string) string {
	if phase == "" {
		return string(metav1.ConditionUnknown)
	}
	return phase
}

func printSection(writer *tabwriter.Writer, title string, headers []string, rows [][]string) {
	fmt.Fprintf(writer, "%s\n", title)
	for index, header := range headers {
		if index > 0 {
			fmt.Fprint(writer, "\t")
		}
		fmt.Fprint(writer, header)
	}
	fmt.Fprint(writer, "\n")

	if len(rows) == 0 {
		fmt.Fprint(writer, "(none)\n\n")
		return
	}

	for _, row := range rows {
		for index, field := range row {
			if index > 0 {
				fmt.Fprint(writer, "\t")
			}
			fmt.Fprint(writer, field)
		}
		fmt.Fprint(writer, "\n")
	}
	fmt.Fprint(writer, "\n")
}

func clusterRows(items []FusekiClusterStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, fmt.Sprintf("%d/%d", item.ReadyReplicas, item.DesiredReplicas), emptyDash(item.ActiveWritePod), emptyDash(item.ReadServiceName), emptyDash(item.WriteServiceName)})
	}
	return rows
}

func deltaRows(items []RDFDeltaStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, fmt.Sprintf("%d/%d", item.ReadyReplicas, item.DesiredReplicas), emptyDash(item.ServiceName), emptyDash(item.BackupCronJobName), emptyDash(item.ActiveRestoreName)})
	}
	return rows
}

func datasetRows(items []DatasetStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, emptyDash(item.ConfigMapName)})
	}
	return rows
}

func endpointRows(items []EndpointStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, emptyDash(item.ReadServiceName), emptyDash(item.WriteServiceName)})
	}
	return rows
}

func backupPolicyRows(items []BackupPolicyStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, emptyDash(item.Schedule), emptyDash(item.Bucket)})
	}
	return rows
}

func restoreRows(items []RestoreRequestStatusSummary) [][]string {
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		rows = append(rows, []string{item.Namespace, item.Name, item.Phase, emptyDash(item.TargetName), emptyDash(item.ResolvedBackupRef), emptyDash(item.JobName)})
	}
	return rows
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
