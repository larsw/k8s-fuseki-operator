# fuseki-operator

`fuseki-operator` manages Apache Jena Fuseki clusters on Kubernetes using RDF Delta for write replication and failover coordination.

This repository currently contains the initial M0 and M1 scaffold:

- a controller-runtime based manager entrypoint
- first-pass CRD API types for `FusekiCluster`, `RDFDeltaServer`, and `Dataset`
- an initial `FusekiCluster` reconciler that creates base config and read/write Services
- config and CI skeletons that can grow into a full Kubebuilder-compatible layout
- a project-owned Fuseki image scaffold under `images/fuseki/`

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

## Custom Fuseki Image

The image scaffold is pinned through [images/fuseki/versions.mk](images/fuseki/versions.mk), which currently tracks Apache Jena Fuseki 6.0.0. You can override those values on the command line if needed:

```sh
make docker-build-fuseki
make docker-smoke-fuseki
```

See [docs/development.md](docs/development.md) for local setup details.
