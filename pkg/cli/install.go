package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/larsw/k8s-fuseki-operator/pkg/version"
)

const officialControllerImage = "ghcr.io/larsw/k8s-fuseki-operator/controller"

type installOptions struct {
	root         *RootOptions
	KustomizeDir string
	Wait         bool
	Timeout      time.Duration
	Namespace    string
	Image        string
	Tag          string
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

			installDir, cleanup, err := prepareInstallKustomizeDir(kustomizeDir, options.Image, options.Tag)
			if err != nil {
				return err
			}
			defer cleanup()

			fmt.Fprintf(cmd.OutOrStdout(), "applying install bundle from %s\n", installDir)
			if err := runKubectl(cmd, options.root, "apply", "-k", installDir); err != nil {
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
	cmd.Flags().StringVar(&options.Image, "image", "", "Full controller image reference to install. Overrides the official image and ignores --tag.")
	cmd.Flags().StringVar(&options.Tag, "tag", "", "Controller image tag to use with the official ghcr.io/larsw/k8s-fuseki-operator/controller image. Defaults to the running fusekictl version.")

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

func prepareInstallKustomizeDir(baseDir, imageOverride, tagOverride string) (string, func(), error) {
	controllerImage := resolveControllerInstallImage(imageOverride, tagOverride)
	overlayDir, err := os.MkdirTemp(filepath.Dir(baseDir), ".fusekictl-install-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		_ = os.RemoveAll(overlayDir)
	}

	basePath, err := filepath.Rel(overlayDir, baseDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}

	override, err := newKustomizeImageOverride(controllerImage)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}

	var builder strings.Builder
	builder.WriteString("apiVersion: kustomize.config.k8s.io/v1beta1\n")
	builder.WriteString("kind: Kustomization\n")
	builder.WriteString("resources:\n")
	builder.WriteString("  - ")
	builder.WriteString(yamlString(filepath.ToSlash(basePath)))
	builder.WriteString("\n")
	builder.WriteString("images:\n")
	builder.WriteString("  - name: ")
	builder.WriteString(yamlString(officialControllerImage))
	builder.WriteString("\n")
	builder.WriteString("    newName: ")
	builder.WriteString(yamlString(override.Name))
	builder.WriteString("\n")
	if override.Tag != "" {
		builder.WriteString("    newTag: ")
		builder.WriteString(yamlString(override.Tag))
		builder.WriteString("\n")
	}
	if override.Digest != "" {
		builder.WriteString("    digest: ")
		builder.WriteString(yamlString(override.Digest))
		builder.WriteString("\n")
	}

	if err := os.WriteFile(filepath.Join(overlayDir, "kustomization.yaml"), []byte(builder.String()), 0o600); err != nil {
		cleanup()
		return "", func() {}, err
	}

	return overlayDir, cleanup, nil
}

func resolveControllerInstallImage(imageOverride, tagOverride string) string {
	if imageOverride != "" {
		return imageOverride
	}
	if tagOverride == "" {
		tagOverride = normalizeDefaultInstallTag(version.Version)
	}
	return officialControllerImage + ":" + tagOverride
}

func normalizeDefaultInstallTag(tag string) string {
	if tag == "" || strings.HasPrefix(tag, "v") {
		return tag
	}
	if tag[0] >= '0' && tag[0] <= '9' {
		return "v" + tag
	}
	return tag
}

type kustomizeImageOverride struct {
	Name   string
	Tag    string
	Digest string
}

func newKustomizeImageOverride(image string) (kustomizeImageOverride, error) {
	name, tag, digest := splitImageReference(image)
	if name == "" {
		return kustomizeImageOverride{}, fmt.Errorf("image reference must not be empty")
	}
	return kustomizeImageOverride{Name: name, Tag: tag, Digest: digest}, nil
}

func splitImageReference(image string) (name, tag, digest string) {
	image = strings.TrimSpace(image)
	if image == "" {
		return "", "", ""
	}
	if base, found := strings.CutSuffix(image, "@"); found {
		return base, "", ""
	}
	if base, found, ok := strings.Cut(image, "@"); ok {
		return base, "", found
	}
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon > lastSlash {
		return image[:lastColon], image[lastColon+1:], ""
	}
	return image, "", ""
}

func yamlString(value string) string {
	return strconv.Quote(value)
}
