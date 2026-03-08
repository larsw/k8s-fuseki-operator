package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type createDatasetOptions struct {
	root         *RootOptions
	Namespace    string
	DatasetName  string
	DisplayName  string
	Type         string
	Spatial      bool
	BackupPolicy string
}

type createRDFDeltaServerOptions struct {
	root         *RootOptions
	Namespace    string
	Image        string
	Replicas     int32
	ServicePort  int32
	BackupPolicy string
}

type createFusekiClusterOptions struct {
	root           *RootOptions
	Namespace      string
	Image          string
	Replicas       int32
	HTTPPort       int32
	RDFDeltaServer string
	Datasets       []string
}

func newCreateCommand(root *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create operator-managed resources",
	}
	cmd.AddCommand(
		newCreateDatasetCommand(root),
		newCreateRDFDeltaServerCommand(root),
		newCreateFusekiClusterCommand(root),
	)
	return cmd
}

func newCreateDatasetCommand(root *RootOptions) *cobra.Command {
	options := &createDatasetOptions{root: root, Type: string(fusekiv1alpha1.DatasetTypeTDB2)}
	cmd := &cobra.Command{
		Use:   "dataset NAME",
		Short: "Create a Dataset resource",
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
			if err := kubeClient.Create(cmd.Context(), dataset); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "created dataset %s in namespace %s\n", dataset.Name, dataset.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the Dataset will be created.")
	cmd.Flags().StringVar(&options.DatasetName, "dataset-name", "", "Spec dataset name. Defaults to the resource name.")
	cmd.Flags().StringVar(&options.DisplayName, "display-name", "", "Display name for the dataset.")
	cmd.Flags().StringVar(&options.Type, "type", options.Type, "Dataset type.")
	cmd.Flags().BoolVar(&options.Spatial, "spatial", false, "Enable Jena Spatial defaults for the dataset.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy name to attach to the dataset.")
	return cmd
}

func newCreateRDFDeltaServerCommand(root *RootOptions) *cobra.Command {
	options := &createRDFDeltaServerOptions{root: root, Replicas: fusekiv1alpha1.DefaultRDFDeltaReplicas, ServicePort: fusekiv1alpha1.DefaultRDFDeltaServicePort}
	cmd := &cobra.Command{
		Use:   "rdfdeltaserver NAME",
		Short: "Create an RDFDeltaServer resource",
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
			if err := kubeClient.Create(cmd.Context(), server); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "created rdfdeltaserver %s in namespace %s\n", server.Name, server.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the RDFDeltaServer will be created.")
	cmd.Flags().StringVar(&options.Image, "image", "", "Container image for the RDFDeltaServer.")
	cmd.Flags().Int32Var(&options.Replicas, "replicas", options.Replicas, "Replica count.")
	cmd.Flags().Int32Var(&options.ServicePort, "service-port", options.ServicePort, "Service port.")
	cmd.Flags().StringVar(&options.BackupPolicy, "backup-policy", "", "BackupPolicy name to attach to the server.")
	return cmd
}

func newCreateFusekiClusterCommand(root *RootOptions) *cobra.Command {
	options := &createFusekiClusterOptions{root: root, Replicas: fusekiv1alpha1.DefaultFusekiReplicas, HTTPPort: fusekiv1alpha1.DefaultFusekiHTTPPort}
	cmd := &cobra.Command{
		Use:   "fusekicluster NAME",
		Short: "Create a FusekiCluster resource",
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
			if err := kubeClient.Create(cmd.Context(), cluster); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "created fusekicluster %s in namespace %s\n", cluster.Name, cluster.Namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace where the FusekiCluster will be created.")
	cmd.Flags().StringVar(&options.Image, "image", "", "Container image for the FusekiCluster.")
	cmd.Flags().Int32Var(&options.Replicas, "replicas", options.Replicas, "Replica count.")
	cmd.Flags().Int32Var(&options.HTTPPort, "http-port", options.HTTPPort, "Fuseki HTTP port.")
	cmd.Flags().StringVar(&options.RDFDeltaServer, "rdf-delta-server", "", "Referenced RDFDeltaServer name.")
	cmd.Flags().StringSliceVar(&options.Datasets, "dataset", nil, "Referenced Dataset name. Repeat to attach multiple datasets.")
	return cmd
}

type datasetParams struct {
	Namespace    string
	ResourceName string
	DatasetName  string
	DisplayName  string
	Type         string
	Spatial      bool
	BackupPolicy string
}

func buildDataset(params datasetParams) *fusekiv1alpha1.Dataset {
	datasetName := params.DatasetName
	if datasetName == "" {
		datasetName = params.ResourceName
	}
	resource := &fusekiv1alpha1.Dataset{}
	resource.Namespace = params.Namespace
	resource.Name = params.ResourceName
	resource.Spec.Name = datasetName
	resource.Spec.Type = fusekiv1alpha1.DatasetType(params.Type)
	resource.Spec.DisplayName = params.DisplayName
	if params.BackupPolicy != "" {
		resource.Spec.BackupPolicyRef = localObjectReference(params.BackupPolicy)
	}
	if params.Spatial {
		resource.Spec.Spatial = &fusekiv1alpha1.JenaSpatialSpec{
			Enabled:          true,
			SpatialIndexPath: "spatial",
		}
	}
	return resource
}

type rdfDeltaServerParams struct {
	Namespace    string
	ResourceName string
	Image        string
	Replicas     int32
	ServicePort  int32
	BackupPolicy string
}

func buildRDFDeltaServer(params rdfDeltaServerParams) *fusekiv1alpha1.RDFDeltaServer {
	resource := &fusekiv1alpha1.RDFDeltaServer{}
	resource.Namespace = params.Namespace
	resource.Name = params.ResourceName
	resource.Spec.Image = params.Image
	resource.Spec.Replicas = params.Replicas
	resource.Spec.ServicePort = params.ServicePort
	if params.BackupPolicy != "" {
		resource.Spec.BackupPolicyRef = localObjectReference(params.BackupPolicy)
	}
	return resource
}

type fusekiClusterParams struct {
	Namespace      string
	ResourceName   string
	Image          string
	Replicas       int32
	HTTPPort       int32
	RDFDeltaServer string
	Datasets       []string
}

func buildFusekiCluster(params fusekiClusterParams) *fusekiv1alpha1.FusekiCluster {
	resource := &fusekiv1alpha1.FusekiCluster{}
	resource.Namespace = params.Namespace
	resource.Name = params.ResourceName
	resource.Spec.Image = params.Image
	resource.Spec.Replicas = params.Replicas
	resource.Spec.HTTPPort = params.HTTPPort
	resource.Spec.RDFDeltaServerRef = corev1.LocalObjectReference{Name: params.RDFDeltaServer}
	for _, dataset := range params.Datasets {
		resource.Spec.DatasetRefs = append(resource.Spec.DatasetRefs, corev1.LocalObjectReference{Name: dataset})
	}
	return resource
}
