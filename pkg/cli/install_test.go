package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/larsw/k8s-fuseki-operator/pkg/version"
)

func TestResolveControllerInstallImageDefaultsToCLIKeyedTag(t *testing.T) {
	originalVersion := version.Version
	version.Version = "0.1.0"
	defer func() {
		version.Version = originalVersion
	}()

	if got := resolveControllerInstallImage("", ""); got != officialControllerImage+":v0.1.0" {
		t.Fatalf("unexpected default controller image: %q", got)
	}
}

func TestResolveControllerInstallImageKeepsNonReleaseDefaultTag(t *testing.T) {
	originalVersion := version.Version
	version.Version = "dev"
	defer func() {
		version.Version = originalVersion
	}()

	if got := resolveControllerInstallImage("", ""); got != officialControllerImage+":dev" {
		t.Fatalf("unexpected default controller image for dev build: %q", got)
	}
}

func TestResolveControllerInstallImageUsesTagOverride(t *testing.T) {
	if got := resolveControllerInstallImage("", "v0.2.0"); got != officialControllerImage+":v0.2.0" {
		t.Fatalf("unexpected controller image for tag override: %q", got)
	}
}

func TestResolveControllerInstallImageUsesImageOverride(t *testing.T) {
	if got := resolveControllerInstallImage("ghcr.io/example/custom-controller:v9.9.9", "v0.2.0"); got != "ghcr.io/example/custom-controller:v9.9.9" {
		t.Fatalf("unexpected controller image for image override: %q", got)
	}
}

func TestPrepareInstallKustomizeDirWritesTaggedImageOverlay(t *testing.T) {
	originalVersion := version.Version
	version.Version = "v1.2.3"
	defer func() {
		version.Version = originalVersion
	}()

	baseDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(baseDir, "kustomization.yaml"), []byte("resources: []\n"), 0o600); err != nil {
		t.Fatalf("write base kustomization: %v", err)
	}

	overlayDir, cleanup, err := prepareInstallKustomizeDir(baseDir, "", "")
	if err != nil {
		t.Fatalf("prepare install overlay: %v", err)
	}
	defer cleanup()

	content, err := os.ReadFile(filepath.Join(overlayDir, "kustomization.yaml"))
	if err != nil {
		t.Fatalf("read overlay kustomization: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, "newName: \"ghcr.io/larsw/k8s-fuseki-operator/controller\"") {
		t.Fatalf("expected official controller image override, got %q", text)
	}
	if !strings.Contains(text, "newTag: \"v1.2.3\"") {
		t.Fatalf("expected version-derived tag override, got %q", text)
	}
	if !strings.Contains(text, "resources:\n  - \"../") {
		t.Fatalf("expected overlay to reference the base kustomization relatively, got %q", text)
	}
}

func TestPrepareInstallKustomizeDirWritesDigestImageOverlay(t *testing.T) {
	baseDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(baseDir, "kustomization.yaml"), []byte("resources: []\n"), 0o600); err != nil {
		t.Fatalf("write base kustomization: %v", err)
	}

	overlayDir, cleanup, err := prepareInstallKustomizeDir(baseDir, "ghcr.io/example/custom-controller@sha256:abcdef", "ignored")
	if err != nil {
		t.Fatalf("prepare install overlay: %v", err)
	}
	defer cleanup()

	content, err := os.ReadFile(filepath.Join(overlayDir, "kustomization.yaml"))
	if err != nil {
		t.Fatalf("read overlay kustomization: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, "newName: \"ghcr.io/example/custom-controller\"") {
		t.Fatalf("expected custom controller repository override, got %q", text)
	}
	if !strings.Contains(text, "digest: \"sha256:abcdef\"") {
		t.Fatalf("expected digest override, got %q", text)
	}
	if strings.Contains(text, "newTag:") {
		t.Fatalf("did not expect tag override when digest is used, got %q", text)
	}
}
