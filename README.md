# fuseki-operator

`fuseki-operator` manages Apache Jena Fuseki on Kubernetes, with support for RDF Delta-backed clustered write coordination, dataset bootstrap, access-control configuration, ingress publication, UI wiring, and backup or restore workflows.

## Status

The project is currently `alpha`.

What that means in practice:

- the core CRDs and reconciler flows are implemented and exercised in local test and k3d end-to-end scenarios
- Helm packaging, release metadata, and OLM bundle packaging exist in-repo
- the API is still `v1alpha1`, so breaking changes are still possible between releases
- production rollout guidance, upgrade guarantees, and support policy are not yet fully documented

Release metadata currently lives in [release/metadata.env](release/metadata.env).

## Current Capabilities

- manage `Dataset`, `RDFDeltaServer`, `FusekiCluster`, `FusekiServer`, `Endpoint`, `FusekiUI`, `BackupPolicy`, and `RestoreRequest` resources
- bootstrap TDB2 datasets, including spatial dataset configuration
- run clustered Fuseki with RDF Delta-backed write coordination
- configure access control with `SecurityProfile` and `SecurityPolicy`
- expose workloads through internal Services and endpoint publication resources
- trigger and inspect backup or restore workflows
- install the operator with `fusekictl`, Kustomize, or Helm
- package the operator for Helm and OLM release flows

## Quick Start

### Local Development

```sh
go mod tidy
make test
make verify
```

Run the controller locally:

```sh
make run
```

Regenerate CRDs after API changes:

```sh
make manifests
```

### Install With Helm

```sh
helm upgrade --install fuseki-operator ./charts/fuseki-operator \
  -n fuseki-system --create-namespace
```

Validate the chart locally with:

```sh
make helm-lint
make helm-test
```

### Install With `fusekictl`

Build the CLI:

```sh
make build-fusekictl
```

Install the operator from the packaged bundle:

```sh
./bin/fusekictl install
```

Or install the latest tagged CLI directly:

```sh
go install github.com/larsw/k8s-fuseki-operator/cmd/fusekictl@latest
```

## Example Workflow

Create a dataset, RDF Delta server, and Fuseki cluster:

```sh
./bin/fusekictl create dataset example-dataset --dataset-name primary --spatial -n default
./bin/fusekictl create rdfdeltaserver example-delta --image ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:v0.1.2 -n default
./bin/fusekictl create fusekicluster example \
  --image ghcr.io/larsw/k8s-fuseki-operator/fuseki:v0.1.2 \
  --rdf-delta-server example-delta \
  --dataset example-dataset \
  -n default
```

Inspect operator and custom-resource state:

```sh
./bin/fusekictl status
```

## Access Control

Authentication, authorization, TLS, OIDC integration, and audit-related behavior are documented in [docs/accesscontrol.md](docs/accesscontrol.md).

## Images

The repository builds and packages project-owned runtime images:

- controller image from `images/controller/Dockerfile`
- Fuseki image from `images/fuseki`
- RDF Delta image from `images/rdf-delta`

The checked-in Fuseki image version inputs live in [images/fuseki/versions.mk](images/fuseki/versions.mk).

Useful local image targets:

```sh
make docker-build-controller
make docker-smoke-controller
make docker-build-fuseki
make docker-smoke-fuseki-all
```

## End-To-End Coverage

The repository includes k3d-backed end-to-end scenarios for core cluster provisioning, recovery, ingress, OIDC, TLS, backup, and restore flows. Examples:

```sh
make e2e-k3d-m3
make e2e-k3d-m3-recovery
make e2e-k3d-fusekiui-ingress
make e2e-k3d-m4-oidc
make e2e-k3d-m4-tls
make e2e-k3d-m5-backup-restore
```

## Packaging And Release

The repository includes:

- a Helm chart under `charts/fuseki-operator`
- an OLM bundle under `bundle/`
- generated release metadata driven from `release/metadata.env`
- CI and release workflows under `.github/workflows/`

Common release preparation commands:

```sh
make release-sync
make release-verify
make bundle-validate
make release-artifacts
```

See [docs/release-packaging.md](docs/release-packaging.md) for packaging details and release workflow notes.

## Known Gaps For Alpha

- API compatibility is not yet stabilized beyond `v1alpha1`
- upgrade guidance and version-to-version compatibility notes still need to be tightened
- OLM metadata is functional but still minimal
- the project does not yet document a formal production support matrix
- audit behavior is currently tied to the underlying Fuseki and Ranger runtime behavior rather than a dedicated operator-managed audit API

## Documentation

- [docs/development.md](docs/development.md): local development and verification
- [docs/fusekictl.md](docs/fusekictl.md): CLI usage
- [docs/release-packaging.md](docs/release-packaging.md): Helm, bundle, and release packaging workflow
- [docs/accesscontrol.md](docs/accesscontrol.md): authentication, authorization, TLS, OIDC, and audit guidance
