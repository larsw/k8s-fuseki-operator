# Release Packaging

The repository now carries the first M6 packaging surfaces for both Helm and OLM:

- Helm chart: `charts/fuseki-operator`
- OLM bundle scaffold: `bundle/`

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

If the cluster needs registry credentials or placement controls, use Helm values for `image.pullSecrets`, `serviceAccount.annotations`, `nodeSelector`, `tolerations`, and `affinity`.

## OLM

Refresh the owned CRDs copied into the bundle and validate the bundle scaffold with:

```sh
make bundle-validate
```

That target refreshes CRDs from `config/crd/bases`, verifies the checked-in CSV and metadata annotations, and runs `operator-sdk bundle validate ./bundle` when `operator-sdk` is available in `PATH`.

Build a bundle image for catalog publishing with:

```sh
make bundle-build BUNDLE_IMAGE=ghcr.io/example/fuseki-operator/bundle:v0.1.0
```

The current bundle is intentionally minimal and uses checked-in metadata. The next packaging step is to derive CSV version fields and related image references from release inputs instead of maintaining them by hand.