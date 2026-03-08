package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type applyDatasetOptions struct {
	root         *RootOptions
	Namespace    string
	DatasetName  string
	DisplayName  string
	Type         string
	Spatial      bool
	BackupPolicy string
}

type applyRDFDeltaServerOptions struct {
	root         *RootOptions
	Namespace    string
	Image        string
	Replicas     int32
	ServicePort  int32
	BackupPolicy string
}

type applyFusekiClusterOptions struct {
	root           *RootOptions
	Namespace      string
	Image          string
	Replicas       int32
	HTTPPort       int32
	RDFDeltaServer string
	Datasets       []string
}

type applyRestoreOptions struct {
	root         *RootOptions
	Namespace    string
	TargetName   string
	BackupObject string
	BackupPolicy string
}

func newApplyCommand(root *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Create or update operator-managed resources",
	}
	cmd.AddCommand(
		newApplyDatasetCommand(root),
		newApplyRDFDeltaServerCommand(root),
		newApplyFusekiClusterCommand(root),
		newApplyRestoreCommand(root),
	)
	return cmd
}

func newApplyDatasetCommand(root *RootOptions) *cobra.Command {
	options := &applyDatasetOptions{root: root, Type: string(fusekiv1alpha1.DatasetTypeTDB2)}
	cmd := &cobra.Command{
		Use:   "dataset NAME",
		Short: "Create or update a Dataset resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			dataset := buildDataset(datasetParams{
				Namespace:    namespace,
				ResourceName: args[0],
				DatasetName:  options.DatasetName,
				DisplayName:  options.DisplayName,
				Type:         options.Type,
				Spatial:      options.Spatial,
				BackupPolicy: options.BackupPolicy,
			})
			action, err := applyDataset(cmd.Context(), kubeClient, dataset)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s dataset %s in namespace %s\n", action, dataset.Name, dataset.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the Dataset will be applied.")
	cmd.Flags().StringVar(&options.DatasetName, "dataset-name", "", "Spec dataset name. Defaults to the resource name.")
	cmd.Flags().StringVar(&options.DisplayName, "display-name", "", "Display name for the dataset.")
	cmd.Flags().StringVar(&options.Type, "type", options.Type, "Dataset type.")
	cmd.Flags().BoolVar(&options.Spatial, "spatial", false, "Enable Jena Spatial defaults for the dataset.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy name to attach to the dataset.")
	return cmd
}

func newApplyRDFDeltaServerCommand(root *RootOptions) *cobra.Command {
	options := &applyRDFDeltaServerOptions{root: root, Replicas: fusekiv1alpha1.DefaultRDFDeltaReplicas, ServicePort: fusekiv1alpha1.DefaultRDFDeltaServicePort}
	cmd := &cobra.Command{
		Use:   "rdfdeltaserver NAME",
		Short: "Create or update an RDFDeltaServer resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.Image == "" {
				return fmt.Errorf("--image is required")
			}
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			server := buildRDFDeltaServer(rdfDeltaServerParams{
				Namespace:    namespace,
				ResourceName: args[0],
				Image:        options.Image,
				Replicas:     options.Replicas,
				ServicePort:  options.ServicePort,
				BackupPolicy: options.BackupPolicy,
			})
			action, err := applyRDFDeltaServer(cmd.Context(), kubeClient, server)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s rdfdeltaserver %s in namespace %s\n", action, server.Name, server.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the RDFDeltaServer will be applied.")
	cmd.Flags().StringVar(&options.Image, "image", "", "Container image for the RDFDeltaServer.")
	cmd.Flags().Int32Var(&options.Replicas, "replicas", options.Replicas, "Replica count.")
	cmd.Flags().Int32Var(&options.ServicePort, "service-port", options.ServicePort, "Service port.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy name to attach to the server.")
	return cmd
}

func newApplyFusekiClusterCommand(root *RootOptions) *cobra.Command {
	options := &applyFusekiClusterOptions{root: root, Replicas: fusekiv1alpha1.DefaultFusekiReplicas, HTTPPort: fusekiv1alpha1.DefaultFusekiHTTPPort}
	cmd := &cobra.Command{
		Use:   "fusekicluster NAME",
		Short: "Create or update a FusekiCluster resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.Image == "" {
				return fmt.Errorf("--image is required")
			}
			if options.RDFDeltaServer == "" {
				return fmt.Errorf("--rdf-delta-server is required")
			}
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			cluster := buildFusekiCluster(fusekiClusterParams{
				Namespace:      namespace,
				ResourceName:   args[0],
				Image:          options.Image,
				Replicas:       options.Replicas,
				HTTPPort:       options.HTTPPort,
				RDFDeltaServer: options.RDFDeltaServer,
				Datasets:       options.Datasets,
			})
			action, err := applyFusekiCluster(cmd.Context(), kubeClient, cluster)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s fusekicluster %s in namespace %s\n", action, cluster.Name, cluster.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the FusekiCluster will be applied.")
	cmd.Flags().StringVar(&options.Image, "image", "", "Container image for the FusekiCluster.")
	cmd.Flags().Int32Var(&options.Replicas, "replicas", options.Replicas, "Replica count.")
	cmd.Flags().Int32Var(&options.HTTPPort, "http-port", options.HTTPPort, "Fuseki HTTP port.")
	cmd.Flags().StringVar(&options.RDFDeltaServer, "rdf-delta-server", "", "Referenced RDFDeltaServer name.")
	cmd.Flags().StringSliceVar(&options.Datasets, "dataset", nil, "Referenced Dataset name. Repeat to attach multiple datasets.")
	return cmd
}

func newApplyRestoreCommand(root *RootOptions) *cobra.Command {
	options := &applyRestoreOptions{root: root}
	cmd := &cobra.Command{
		Use:   "restore NAME",
		Short: "Create or update a RestoreRequest resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.TargetName == "" {
				return fmt.Errorf("--target is required")
			}
			kubeClient, err := options.root.kubeClient()
			if err != nil {
				return err
			}
			namespace, err := namespaceOrCurrent(options.root, options.Namespace)
			if err != nil {
				return err
			}
			request := buildRestoreRequest(restoreRequestParams{
				Namespace:    namespace,
				Name:         args[0],
				TargetName:   options.TargetName,
				BackupObject: options.BackupObject,
				BackupPolicy: options.BackupPolicy,
			})
			action, err := applyRestoreRequest(cmd.Context(), kubeClient, request)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s restore %s in namespace %s\n", action, request.Name, request.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the RestoreRequest will be applied.")
	cmd.Flags().StringVar(&options.TargetName, "target", "", "Referenced RDFDeltaServer name.")
	cmd.Flags().StringVar(&options.BackupObject, "backup-object", "", "Backup object to restore. Defaults to the controller's latest selection.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy name to use for the restore.")
	return cmd
}

func applyDataset(ctx context.Context, kubeClient client.Client, desired *fusekiv1alpha1.Dataset) (string, error) {
	existing := &fusekiv1alpha1.Dataset{}
	err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return "created", kubeClient.Create(ctx, desired)
	}
	if err != nil {
		return "", err
	}
	existing.Spec = desired.Spec
	return "configured", kubeClient.Update(ctx, existing)
}

func applyRDFDeltaServer(ctx context.Context, kubeClient client.Client, desired *fusekiv1alpha1.RDFDeltaServer) (string, error) {
	existing := &fusekiv1alpha1.RDFDeltaServer{}
	err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return "created", kubeClient.Create(ctx, desired)
	}
	if err != nil {
		return "", err
	}
	existing.Spec = desired.Spec
	return "configured", kubeClient.Update(ctx, existing)
}

func applyFusekiCluster(ctx context.Context, kubeClient client.Client, desired *fusekiv1alpha1.FusekiCluster) (string, error) {
	existing := &fusekiv1alpha1.FusekiCluster{}
	err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return "created", kubeClient.Create(ctx, desired)
	}
	if err != nil {
		return "", err
	}
	existing.Spec = desired.Spec
	return "configured", kubeClient.Update(ctx, existing)
}

func applyRestoreRequest(ctx context.Context, kubeClient client.Client, desired *fusekiv1alpha1.RestoreRequest) (string, error) {
	existing := &fusekiv1alpha1.RestoreRequest{}
	err := kubeClient.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return "created", kubeClient.Create(ctx, desired)
	}
	if err != nil {
		return "", err
	}
	existing.Spec = desired.Spec
	return "configured", kubeClient.Update(ctx, existing)
}
