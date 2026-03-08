# Release Packaging

The repository now carries the first M6 packaging surfaces for both Helm and OLM:

- Helm chart: `charts/fuseki-operator`
- OLM bundle scaffold: `bundle/`
- Release metadata source: `release/metadata.env`
- GitHub release workflows: `.github/workflows/ci.yaml` and `.github/workflows/release.yaml`

The checked-in chart metadata, default chart image tag, bundle annotations, and CSV release fields are generated from `release/metadata.env` via:

```sh
make release-sync
```

CI also enforces that the generated packaging files stay in sync:

```sh
make release-verify
```

## Helm

Validate the chart locally with:

```sh
make helm-lint
make helm-test
```

For released controller images, override `image.tag` at install or upgrade time:

```sh
helm upgrade --install fuseki-operator ./charts/fuseki-operator \
  -n fuseki-system --create-namespace \
  --set image.tag=v0.1.0
```

When cutting a release, update `release/metadata.env` first and then refresh the generated packaging files with `make release-sync`.

If the cluster needs registry credentials or placement controls, use Helm values for `image.pullSecrets`, `serviceAccount.annotations`, `nodeSelector`, `tolerations`, and `affinity`.

## Branch Model

- `main`: integration branch. All changes land through pull requests and must pass CI.
- `release/x.y`: stabilization branch for an upcoming `x.y.z` release line. Only release prep and targeted fixes should land here.
- `vX.Y.Z` tags: cut from `main` or the matching `release/x.y` branch after `release/metadata.env` has been updated to the intended release metadata.

The CI workflow now runs on both `main` and `release/**` branches so release stabilization gets the same validation bar as integration work.

Suggested repository settings:

- protect `main` and `release/*`
- require pull requests and the `ci` workflow to pass before merge
- limit direct tag creation to maintainers

## OLM

Refresh the owned CRDs copied into the bundle and validate the bundle scaffold with:

```sh
make bundle-validate
```

That target refreshes CRDs from `config/crd/bases`, verifies the checked-in CSV and metadata annotations, and runs `operator-sdk bundle validate ./bundle` when `operator-sdk` is available in `PATH`.

Build a bundle image for catalog publishing with:

```sh
make bundle-build BUNDLE_IMAGE=ghcr.io/larsw/k8s-fuseki-operator/bundle:v0.1.0
```

The current bundle is still intentionally minimal, but its CSV version, bundle channels, release timestamp, controller image, and related image references now come from `release/metadata.env` instead of being repeated by hand across packaging files.

## Release Artifacts

Build the local release artifact set with:

```sh
make release-artifacts
```

That target currently packages:

- the Helm chart tarball
- a tarball containing the OLM bundle scaffold
- `fusekictl` binaries for Linux and macOS on amd64 and arm64
- a `checksums.txt` file for the generated artifacts

Pushing a `vX.Y.Z` tag triggers `.github/workflows/release.yaml`, which validates the repo, builds the same artifact set, uploads it to the workflow run, and creates a draft GitHub Release.