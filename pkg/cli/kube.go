package cli

import (
	"fmt"
	"os"
	"path/filepath"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

const (
	DefaultOperatorNamespace = "fuseki-system"
	OperatorDeploymentName   = "fuseki-operator-controller-manager"
)

var kubeScheme = runtimeScheme()

func runtimeScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = fusekiv1alpha1.AddToScheme(scheme)
	return scheme
}

func (o *RootOptions) kubeClient() (client.Client, error) {
	config, err := o.restConfig()
	if err != nil {
		return nil, err
	}

	return client.New(config, client.Options{Scheme: kubeScheme})
}

func (o *RootOptions) kubernetesClientset() (*kubernetes.Clientset, error) {
	config, err := o.restConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func (o *RootOptions) restConfig() (*rest.Config, error) {
	return o.clientConfig().ClientConfig()
}

func (o *RootOptions) currentNamespace() (string, error) {
	namespace, _, err := o.clientConfig().Namespace()
	if err != nil {
		return "", err
	}
	if namespace == "" {
		return "default", nil
	}
	return namespace, nil
}

func (o *RootOptions) clientConfig() clientcmd.ClientConfig {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if o.Kubeconfig != "" {
		loadingRules.ExplicitPath = o.Kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{}
	if o.Context != "" {
		overrides.CurrentContext = o.Context
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
}

func namespaceOrCurrent(root *RootOptions, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	return root.currentNamespace()
}

func localObjectReference(name string) *corev1.LocalObjectReference {
	if name == "" {
		return nil
	}
	return &corev1.LocalObjectReference{Name: name}
}

func resolveKustomizeDir(explicitDir string) (string, error) {
	if explicitDir != "" {
		return ensureKustomizeDir(explicitDir)
	}

	if envDir := os.Getenv("FUSEKICTL_KUSTOMIZE_DIR"); envDir != "" {
		return ensureKustomizeDir(envDir)
	}

	searchRoots := make([]string, 0, 2)
	if cwd, err := os.Getwd(); err == nil {
		searchRoots = append(searchRoots, cwd)
	}
	if exePath, err := os.Executable(); err == nil {
		searchRoots = append(searchRoots, filepath.Dir(exePath))
	}

	for _, root := range searchRoots {
		if dir, ok := searchUpwardsForKustomizeDir(root); ok {
			return dir, nil
		}
	}

	return "", fmt.Errorf("could not locate config/default; pass --kustomize-dir or set FUSEKICTL_KUSTOMIZE_DIR")
}

func searchUpwardsForKustomizeDir(start string) (string, bool) {
	current := start
	for {
		candidate := filepath.Join(current, "config", "default")
		if _, err := os.Stat(filepath.Join(candidate, "kustomization.yaml")); err == nil {
			return candidate, true
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", false
		}
		current = parent
	}
}

func ensureKustomizeDir(dir string) (string, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(absDir, "kustomization.yaml")); err != nil {
		return "", fmt.Errorf("kustomize directory %q does not contain kustomization.yaml", absDir)
	}
	return absDir, nil
}
