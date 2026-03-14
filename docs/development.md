# Development

## Prerequisites

- Go 1.25+
- Docker
- `k3d` for end-to-end work later in the roadmap

Optional local tools:

- `controller-gen`
- `kustomize`
- `kubebuilder`
- `helm`

The scaffold does not require the binaries above to exist globally. The `Makefile` uses `go run` for controller generation so the tool can be fetched on demand.

## Bootstrap

```sh
go mod tidy
make test
```

For the broader local verification sweep used before pushing changes, run:

```sh
make verify
```

This target runs `go vet`, the full Go test suite, the controller image smoke test, the aggregate Fuseki smoke suite including the Ranger path, and the RDF Delta smoke test.

## Run The Manager

```sh
make run
```

The manager starts health and readiness probes and currently registers the first `FusekiCluster` reconciler.

## Lint The Helm Chart

The repository now includes an initial chart under `charts/fuseki-operator`.

```sh
make helm-lint
make helm-test
helm template fuseki-operator ./charts/fuseki-operator -n fuseki-system >/tmp/fuseki-operator-chart.yaml
```

The Helm test target renders the chart with both default values and override values so the checked-in chart surface stays covered as M6 packaging grows.

The chart now exposes service account annotations plus the core scheduling controls (`nodeSelector`, `tolerations`, and `affinity`). Controller image tags remain overrideable through `image.tag`, which is the expected path for release installs.

## Validate The OLM Bundle

The repository now also includes an initial OLM bundle scaffold under `bundle/`.

```sh
make bundle-validate
```

The bundle validation script checks the checked-in CSV, metadata annotations, and owned CRD list. If `operator-sdk` is installed locally, it also runs `operator-sdk bundle validate ./bundle` as an upstream validation pass.

## Build The Custom Fuseki Image

The repository now includes a checked-in [images/fuseki/versions.mk](../images/fuseki/versions.mk) with the current verified Apache Jena Fuseki release inputs. Build the image with:

```sh
make docker-build-fuseki
make docker-smoke-fuseki-all
```

The aggregate Fuseki smoke target runs both the base Fuseki smoke check and the Ranger-backed authorization smoke check. The Ranger path now talks to a live Apache Ranger admin and bootstraps smoke-specific Ranger objects before it starts Fuseki.

To start a local Ranger stack that matches CI, use:

```sh
bash ./hack/smoke/ranger-stack.sh up
```

The helper uses the upstream Ranger images' expected internal hostnames on the `rangernw` Docker network. If you already have a local `ranger-admin` stack running, use that existing stack directly instead of starting a second one with the helper.

Then run:

```sh
make docker-smoke-fuseki-ranger
```

The Ranger smoke flow expects a live Ranger admin reachable at `http://127.0.0.1:16080/service` with `admin` and `rangerR0cks!` by default. It bootstraps a smoke-specific Fuseki service definition, a `fuseki-smoke` service, test users, a test group, a test role, and the policies needed for the existing allow and deny checks. Override `RANGER_ADMIN_URL`, `RANGER_USERNAME`, `RANGER_PASSWORD`, `RANGER_SERVICE_NAME`, `RANGER_SERVICE_DEF_NAME`, or `RANGER_ADMIN_CONTAINER_URL` when your local stack differs.

When you are done with the local Ranger stack, stop it with:

```sh
bash ./hack/smoke/ranger-stack.sh down
```

To override the pinned release for testing, pass `JENA_VERSION` and `JENA_SHA512` explicitly on the command line.

## Run The First k3d M3 Scenario

The repository now includes a first k3d-backed M3 scenario that runs the manager locally against a disposable k3d cluster, builds the local Fuseki image, builds the in-repo RDF Delta image for the test harness, applies CRDs and example resources, and verifies:

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

1. Expand the Helm chart with upgrade guidance.
2. Deepen the OLM metadata and automate CSV generation from release inputs.
3. Add release docs, upgrade notes, and artifact publishing workflows.
