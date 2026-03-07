# Development

## Prerequisites

- Go 1.25+
- Docker
- `k3d` for end-to-end work later in the roadmap

Optional local tools:

- `controller-gen`
- `kustomize`
- `kubebuilder`

The scaffold does not require the binaries above to exist globally. The `Makefile` uses `go run` for controller generation so the tool can be fetched on demand.

## Bootstrap

```sh
go mod tidy
make test
```

## Run The Manager

```sh
make run
```

The manager starts health and readiness probes and currently registers the first `FusekiCluster` reconciler.

## Build The Custom Fuseki Image

The repository now includes a checked-in [images/fuseki/versions.mk](../images/fuseki/versions.mk) with the current verified Apache Jena Fuseki release inputs. Build the image with:

```sh
make docker-build-fuseki
make docker-smoke-fuseki
```

To override the pinned release for testing, pass `JENA_VERSION` and `JENA_SHA512` explicitly on the command line.

## Run The First k3d M3 Scenario

The repository now includes a first k3d-backed M3 scenario that runs the manager locally against a disposable k3d cluster, builds the local Fuseki image, builds a small mock RDF Delta image for the test harness, applies CRDs and example resources, and verifies:

- RDF Delta and Fuseki workloads become ready
- the write lease selects a single pod
- read and write Services expose the expected endpoint fanout
- the dataset bootstrap job completes
- authenticated writes succeed through the write Service

Run it with:

```sh
make e2e-k3d-m3
```

Keep the cluster around for inspection with:

```sh
KEEP_CLUSTER=1 make e2e-k3d-m3
```

## Next Scaffold Steps

1. Expand the remaining CRD types beyond the first `FusekiCluster`, `RDFDeltaServer`, and `Dataset` field sets.
2. Add stateful workload reconciliation for Fuseki and RDF Delta.
3. Add k3d automation and end-to-end coverage.
4. Add backup and restore workflow implementation.
