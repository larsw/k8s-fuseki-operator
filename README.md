# fuseki-operator

`fuseki-operator` manages Apache Jena Fuseki clusters on Kubernetes using RDF Delta for write replication and failover coordination.

This repository now includes working M3 through M5 operator flows plus the first M6 CLI slice:

- controller-runtime based reconcilers for clusters, datasets, RDF Delta, security, endpoints, UI, and backup/restore
- k3d end-to-end scenarios for M3, M4, and M5 user flows
- project-owned Fuseki and RDF Delta images under `images/`
- a repo-packaged install bundle under `config/default`
- a `fusekictl` CLI for install, uninstall, status, restore inspection and logs, backup trigger, and typed resource lifecycle workflows
- an initial Helm chart under `charts/fuseki-operator` for operator installation
- an initial OLM bundle scaffold under `bundle/` for release packaging

## Module Path

The Go module path is currently set to `fuseki-operator` so the repository can be scaffolded before a final git remote exists. If this project gets published under a different canonical import path, update it with:

```sh
go mod edit -module <final-module-path>
go mod tidy
```

## Quick Start

```sh
go mod tidy
make test
make run
```

Generate CRDs after API changes with:

```sh
make manifests
```

Run the first k3d-backed M3 end-to-end scenario with:

```sh
make e2e-k3d-m3
```

Build the CLI with:

```sh
make build-fusekictl
```

Install the operator from the packaged bundle with:

```sh
./bin/fusekictl install
```

Install the operator with Helm using the initial chart with:

```sh
helm install fuseki-operator ./charts/fuseki-operator -n fuseki-system --create-namespace
make helm-test
```

Override the controller image tag or pin scheduling-related values with a small values file or `--set`, for example:

```sh
helm upgrade --install fuseki-operator ./charts/fuseki-operator \
	-n fuseki-system --create-namespace \
	--set image.tag=v0.1.0 \
	--set nodeSelector."kubernetes\.io/os"=linux
```

Validate the initial OLM bundle scaffold with:

```sh
make bundle-validate
```

## Custom Fuseki Image

The image scaffold is pinned through [images/fuseki/versions.mk](images/fuseki/versions.mk), which currently tracks Apache Jena Fuseki 6.0.0. You can override those values on the command line if needed:

```sh
make docker-build-fuseki
make docker-smoke-fuseki
```

See [docs/development.md](docs/development.md) for local setup details, [docs/fusekictl.md](docs/fusekictl.md) for CLI usage, and [docs/release-packaging.md](docs/release-packaging.md) for Helm and OLM packaging workflow notes.
