package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"fuseki-operator/pkg/version"
)

type RootOptions struct {
	Kubeconfig string
	Context    string
}

func Execute() error {
	return NewRootCommand().Execute()
}

func NewRootCommand() *cobra.Command {
	root := &RootOptions{}
	cmd := &cobra.Command{
		Use:          "fusekictl",
		Short:        "Operate fuseki-operator installations and resources",
		SilenceUsage: true,
	}

	cmd.PersistentFlags().StringVar(&root.Kubeconfig, "kubeconfig", "", "Path to the kubeconfig file.")
	cmd.PersistentFlags().StringVar(&root.Context, "context", "", "Kubeconfig context to use.")

	cmd.AddCommand(
		newInstallCommand(root),
		newUninstallCommand(root),
		newStatusCommand(root),
		newBackupCommand(root),
		newRestoreCommand(root),
		newApplyCommand(root),
		newCreateCommand(root),
		newDeleteCommand(root),
		newVersionCommand(),
	)

	return cmd
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the fusekictl version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(cmd.OutOrStdout(), version.String())
		},
	}
}
