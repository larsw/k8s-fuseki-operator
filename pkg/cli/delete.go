package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "fuseki-operator/api/v1alpha1"
)

type deleteOptions struct {
	root           *RootOptions
	Namespace      string
	IgnoreNotFound bool
}

func newDeleteCommand(root *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete operator-managed resources",
	}
	cmd.AddCommand(
		newDeleteDatasetCommand(root),
		newDeleteRDFDeltaServerCommand(root),
		newDeleteFusekiClusterCommand(root),
		newDeleteRestoreRequestCommand(root),
	)
	return cmd
}

func newDeleteDatasetCommand(root *RootOptions) *cobra.Command {
	return newDeleteResourceCommand(root, "dataset", func(namespace, name string) client.Object {
		return &fusekiv1alpha1.Dataset{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	})
}

func newDeleteRDFDeltaServerCommand(root *RootOptions) *cobra.Command {
	return newDeleteResourceCommand(root, "rdfdeltaserver", func(namespace, name string) client.Object {
		return &fusekiv1alpha1.RDFDeltaServer{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	})
}

func newDeleteFusekiClusterCommand(root *RootOptions) *cobra.Command {
	return newDeleteResourceCommand(root, "fusekicluster", func(namespace, name string) client.Object {
		return &fusekiv1alpha1.FusekiCluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	})
}

func newDeleteRestoreRequestCommand(root *RootOptions) *cobra.Command {
	return newDeleteResourceCommand(root, "restore", func(namespace, name string) client.Object {
		return &fusekiv1alpha1.RestoreRequest{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	})
}

func newDeleteResourceCommand(root *RootOptions, resourceName string, objectForName func(namespace, name string) client.Object) *cobra.Command {
	options := &deleteOptions{root: root}
	cmd := &cobra.Command{
		Use:   resourceName + " NAME",
		Short: "Delete a " + resourceName + " resource",
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
			obj := objectForName(namespace, args[0])
			err = kubeClient.Delete(cmd.Context(), obj)
			if options.IgnoreNotFound && apierrors.IsNotFound(err) {
				fmt.Fprintf(cmd.OutOrStdout(), "%s %s was already absent in namespace %s\n", resourceName, args[0], namespace)
				return nil
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted %s %s from namespace %s\n", resourceName, args[0], namespace)
			return nil
		},
	}
	cmd.Flags().StringVarP(&options.Namespace, "namespace", "n", "", "Namespace containing the resource.")
	cmd.Flags().BoolVar(&options.IgnoreNotFound, "ignore-not-found", false, "Treat a missing resource as success.")
	return cmd
}
