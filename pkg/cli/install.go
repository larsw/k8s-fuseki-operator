package cli

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/spf13/cobra"
)

type installOptions struct {
	root         *RootOptions
	KustomizeDir string
	Wait         bool
	Timeout      time.Duration
	Namespace    string
}

func newInstallCommand(root *RootOptions) *cobra.Command {
	options := &installOptions{
		root:      root,
		Wait:      true,
		Timeout:   2 * time.Minute,
		Namespace: DefaultOperatorNamespace,
	}

	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install the operator manifests into the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			kustomizeDir, err := resolveKustomizeDir(options.KustomizeDir)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "applying install bundle from %s\n", kustomizeDir)
			if err := runKubectl(cmd, options.root, "apply", "-k", kustomizeDir); err != nil {
				return err
			}

			if !options.Wait {
				return nil
			}

			fmt.Fprintf(cmd.OutOrStdout(), "waiting for deployment/%s in namespace %s\n", OperatorDeploymentName, options.Namespace)
			return runKubectl(
				cmd,
				options.root,
				"wait",
				"--namespace", options.Namespace,
				"--for=condition=Available",
				"--timeout="+options.Timeout.String(),
				"deployment/"+OperatorDeploymentName,
			)
		},
	}

	cmd.Flags().StringVar(&options.KustomizeDir, "kustomize-dir", "", "Path to the operator kustomize install directory.")
	cmd.Flags().BoolVar(&options.Wait, "wait", options.Wait, "Wait for the operator deployment to become available.")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Maximum time to wait for operator availability.")
	cmd.Flags().StringVar(&options.Namespace, "operator-namespace", options.Namespace, "Namespace where the operator deployment is installed.")

	return cmd
}

func newUninstallCommand(root *RootOptions) *cobra.Command {
	options := &installOptions{
		root:      root,
		Namespace: DefaultOperatorNamespace,
	}

	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove the operator manifests from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			kustomizeDir, err := resolveKustomizeDir(options.KustomizeDir)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "deleting install bundle from %s\n", kustomizeDir)
			return runKubectl(cmd, options.root, "delete", "-k", kustomizeDir, "--ignore-not-found=true", "--wait=false")
		},
	}

	cmd.Flags().StringVar(&options.KustomizeDir, "kustomize-dir", "", "Path to the operator kustomize install directory.")

	return cmd
}

func runKubectl(cmd *cobra.Command, root *RootOptions, args ...string) error {
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		return fmt.Errorf("kubectl is required: %w", err)
	}

	invocation := exec.CommandContext(cmd.Context(), kubectlPath, append(kubectlArgs(root), args...)...)
	invocation.Stdout = cmd.OutOrStdout()
	invocation.Stderr = cmd.ErrOrStderr()
	return invocation.Run()
}

func kubectlArgs(root *RootOptions) []string {
	args := make([]string, 0, 4)
	if root.Kubeconfig != "" {
		args = append(args, "--kubeconfig", root.Kubeconfig)
	}
	if root.Context != "" {
		args = append(args, "--context", root.Context)
	}
	return args
}
