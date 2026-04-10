# Access Control

This document describes the authentication, authorization, TLS, OIDC, and audit-related features currently exposed by `fuseki-operator`.

The operator's access-control model is centered on two resource types:

- `SecurityProfile`: shared runtime security configuration such as admin credentials, TLS material, OIDC metadata, and authorization mode
- `SecurityPolicy`: dataset-level authorization rules used in `Local` authorization mode

At a high level:

- authentication is provided through admin credentials, TLS, and external identity integration metadata
- authorization is provided either by local policy bundles or Apache Ranger integration
- audit is not yet a first-class operator API; any audit behavior comes from the underlying Fuseki runtime and, in Ranger mode, the Ranger deployment you integrate with

## Concepts

### `SecurityProfile`

`SecurityProfile` is the main shared security object. Workloads reference it through `spec.securityProfileRef`.

Current `SecurityProfile` fields include:

- `adminCredentialsSecretRef`
- `tlsSecretRef`
- `oidc.issuerURL`
- `oidc.clientID`
- `oidc.tlsCASecretRef`
- `authorization.mode`
- `authorization.ranger.*`

The reconciler projects the profile into a ConfigMap and mounts or injects the referenced secrets into Fuseki workloads.

### `SecurityPolicy`

`SecurityPolicy` expresses dataset-level authorization rules. A `Dataset` attaches policies through `spec.securityPolicies`.

A `SecurityPolicy` has two mutually-independent ways to express authorization rules. At least one must be present.

#### Static rules (`spec.rules`)

Each rule statically enumerates a target and specifies how access is controlled:

- a dataset target, optionally narrowed to a named graph
- a set of subjects
- a set of actions
- an allow or deny effect
- an expression type and expression string

#### Dynamic graph tagging (`spec.graphTagging`)

Each entry defines dataset-scoped, RDF*-based dynamic authorization. Instead of naming specific graphs in the manifest, the security expression is embedded directly on each named graph as an RDF* annotation. At request time the runtime reads the expression from the graph node, evaluates it against the requesting subject's authorizations, and allows or denies access accordingly.

This is useful when named graphs are created dynamically and should inherit their access control from metadata embedded in the data itself.

See [Dynamic Graph Security via RDF* Tagging](#dynamic-graph-security-via-rdf-tagging) for full details.

Current subject types:

- `User`
- `Group`
- `OIDCClaim`

Current actions:

- `Query` — SPARQL query endpoint
- `Update` — SPARQL update endpoint
- `Read` — Graph Store Protocol GET
- `Write` — Graph Store Protocol PUT / POST / PATCH / DELETE
- `Admin` — administrative dataset operations

Current effects (static rules only):

- `Allow`
- `Deny`

Current expression types:

- `Simple`
- `Accumulo`

## Authentication

### Admin Credentials

Admin credentials are referenced from `SecurityProfile.spec.adminCredentialsSecretRef`.

Recommended secret shape:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: example-admin-secret
  namespace: default
stringData:
  username: admin
  password: change-me
```

How the operator uses this secret:

- bootstrap jobs read `username` and `password` when creating datasets or preloading data
- the Fuseki runtime currently consumes the `password` value as `ADMIN_PASSWORD`
- using `username: admin` is the expected path and matches the image bootstrap behavior

Operational note:

- if admin credentials are not configured, bootstrap jobs can still render configuration, but authenticated dataset creation is skipped
- this is why bootstrap logs can say `Admin credentials are not configured; skipping authenticated dataset creation.`

### TLS

TLS material is referenced from `SecurityProfile.spec.tlsSecretRef`.

Expected secret shape:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: example-tls-secret
  namespace: default
type: kubernetes.io/tls
data:
  tls.crt: ...
  tls.key: ...
```

Current reconciler validation requires:

- `tls.crt`
- `tls.key`

The security runtime also writes paths for:

- `tls.certFile=/fuseki-extra/security/tls/tls.crt`
- `tls.keyFile=/fuseki-extra/security/tls/tls.key`
- `tls.caFile=/fuseki-extra/security/tls/ca.crt`

`ca.crt` may be mounted if present, but the main hard requirement today is the standard TLS key pair.

### OIDC Metadata

OIDC metadata is configured on the `SecurityProfile`:

```yaml
spec:
  oidc:
    issuerURL: https://issuer.example.com/realms/fuseki
    clientID: fuseki-ui
```

What the operator does:

- propagates the issuer URL and client ID into runtime configuration and environment variables
- makes this information available to the Fuseki image at startup

What the operator does not do:

- it does not provision an OIDC provider
- it does not create clients, redirect URIs, groups, or claims in your identity provider
- it does not manage user lifecycle

Treat the operator as the place where Fuseki-side OIDC metadata is wired, not the place where the IdP itself is administered.

#### OIDC Provider TLS Trust Anchor

If your OIDC provider uses a certificate issued by a private or internal CA that is not in the standard trust store of the Fuseki image, you can reference the root certificate directly on the `SecurityProfile`:

```yaml
spec:
  oidc:
    issuerURL: https://idp.internal.example.com/realms/fuseki
    clientID: fuseki-ui
    tlsCASecretRef:
      name: oidc-ca-cert
      key: ca.crt
```

The referenced secret key must contain a PEM-encoded certificate (or certificate chain) that serves as the trust anchor for verifying the OIDC provider's TLS endpoints. The operator mounts or injects this material into the Fuseki runtime so that OIDC discovery and token-validation requests succeed against the private CA.

Recommended secret shape:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: oidc-ca-cert
  namespace: default
data:
  ca.crt: <base64-encoded PEM certificate>
```

The `optional` field on `tlsCASecretRef` controls whether the operator treats a missing secret as a hard failure. When omitted or `false`, the profile will not become `Ready` until the secret exists.

## Authorization

Two authorization modes are supported:

- `Local`
- `Ranger`

If you do not set `authorization.mode`, the operator defaults to `Local`.

### Local Authorization

In `Local` mode, dataset-level `SecurityPolicy` attachments are collected into JSON bundles and indexed for the Fuseki runtime.

Important behavior:

- the runtime builds an authorization index under `/fuseki-extra/authorization`
- the runtime is configured with `authorization.failClosed=true`
- if a referenced `SecurityPolicy` is missing, the runtime refuses to start rather than silently allowing access

This is an intentional fail-closed design.

#### Example: Local Authorization Profile

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: SecurityProfile
metadata:
  name: local-security
  namespace: default
spec:
  adminCredentialsSecretRef:
    name: example-admin-secret
  tlsSecretRef:
    name: example-tls-secret
  oidc:
    issuerURL: https://issuer.example.com/realms/fuseki
    clientID: fuseki-ui
  authorization:
    mode: Local
```

#### Example: Dataset With Attached Security Policies

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: Dataset
metadata:
  name: example-dataset
  namespace: default
spec:
  name: primary
  type: TDB2
  spatial:
    enabled: true
    assembler: spatial:EntityMap
    spatialIndexPath: spatial
  securityPolicies:
    - name: analysts-read
```

#### Example: Local Security Policy — Static Rules

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: SecurityPolicy
metadata:
  name: analysts-read
  namespace: default
spec:
  description: Allow analysts to query a public named graph.
  rules:
    - target:
        datasetRef:
          name: example-dataset
        namedGraph: https://example.com/graphs/public
      subjects:
        - type: Group
          value: analysts
      actions:
        - Query
        - Read
      effect: Allow
      expressionType: Simple
      expression: PUBLIC
```

## Dynamic Graph Security via RDF* Tagging

In addition to static `rules`, a `SecurityPolicy` can use `spec.graphTagging` to define access control that is driven by security expressions embedded directly in the RDF graph data as RDF* annotations.

### Motivation

Static rules work well when the set of named graphs is known at deployment time. They break down when named graphs are created dynamically — for example by ingest pipelines — and each graph carries its own classification that must be enforced at query time.

RDF* tagging solves this by annotating each named graph node with the security expression it requires:

```turtle
<< <https://example.com/graphs/project-x> a :SecuredGraph >>
    <https://fuseki.apache.org/security#expression> "TopSecret&Project-X" .
```

The policy manifest then declares only *who* is governed by those annotations and *what operations* they affect — the runtime evaluates access per-graph based on the embedded expression.

### API

```yaml
spec:
  graphTagging:
    - datasetRef:
        name: sensitive-dataset
      expressionType: Accumulo         # Simple or Accumulo
      tagPredicate: "https://fuseki.apache.org/security#expression"  # optional default
      actions:
        - Query
        - Read
      subjects:
        - type: Group
          value: data-scientists
        - type: OIDCClaim
          claim: roles
```

`graphTagging` fields:

| Field | Required | Description |
|---|---|---|
| `datasetRef` | yes | dataset whose named graphs carry RDF* annotations |
| `expressionType` | no (default `Simple`) | expression dialect used in the annotations: `Simple` or `Accumulo` |
| `tagPredicate` | no | RDF predicate used in RDF* annotations; defaults to `https://fuseki.apache.org/security#expression` |
| `actions` | yes | dataset operations governed by the dynamic expressions |
| `subjects` | yes | principals evaluated against the embedded expressions |

### Combining Static Rules And Graph Tagging

Static `rules` and `graphTagging` entries can coexist in the same `SecurityPolicy`. A policy may consist entirely of `graphTagging` entries with no static rules.

Example combining both approaches:

```yaml
spec:
  description: Static admin grant + dynamic per-graph classification
  rules:
    - target:
        datasetRef:
          name: sensitive-dataset
      subjects:
        - type: User
          value: data-admin
      actions:
        - Query
        - Update
        - Read
        - Write
        - Admin
      effect: Allow
      expressionType: Simple
      expression: "*"
  graphTagging:
    - datasetRef:
        name: sensitive-dataset
      expressionType: Accumulo
      actions:
        - Query
        - Read
      subjects:
        - type: Group
          value: analysts
```

### Expression Types In RDF* Annotations

- **Simple**: plain label strings where `*` means unrestricted and `PUBLIC` means unauthenticated access is allowed
- **Accumulo**: label-based visibility expressions following the Apache Accumulo syntax, e.g. `TopSecret&Project-X|(Unclassified&Public)`

The `expressionType` on the `graphTagging` entry must match the dialect used in the RDF* annotations in the data.

#### Example: Attach The Profile To A Cluster

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: FusekiCluster
metadata:
  name: example
  namespace: default
spec:
  replicas: 3
  image: ghcr.io/larsw/k8s-fuseki-operator/fuseki:v0.1.2
  httpPort: 3030
  rdfDeltaServerRef:
    name: example-delta
  datasetRefs:
    - name: example-dataset
  securityProfileRef:
    name: local-security
  storage:
    accessMode: ReadWriteOnce
    size: 2Gi
```

### Ranger Authorization

In `Ranger` mode, the operator configures Fuseki to integrate with Apache Ranger instead of consuming local dataset policy bundles.

Required Ranger settings:

- `authorization.mode: Ranger`
- `authorization.ranger.adminURL`
- `authorization.ranger.serviceName`
- `authorization.ranger.authSecretRef`

The referenced secret must contain:

- `username`
- `password`

Optional Ranger tuning:

- `authorization.ranger.pollInterval`

#### Example: Ranger Security Profile

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: SecurityProfile
metadata:
  name: ranger-security
  namespace: default
spec:
  authorization:
    mode: Ranger
    ranger:
      adminURL: https://ranger-admin.example.internal
      serviceName: fuseki-prod
      authSecretRef:
        name: ranger-admin-credentials
      pollInterval: 45s
  oidc:
    issuerURL: https://issuer.example.com/realms/fuseki
    clientID: fuseki-ui
```

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ranger-admin-credentials
  namespace: default
stringData:
  username: admin
  password: rangerR0cks!
```

#### Ranger Constraint

Ranger mode cannot be combined with local dataset `SecurityPolicy` bundles.

That constraint is enforced in multiple places:

- admission and validation reject invalid combinations
- runtime startup exits rather than mixing Ranger with local policy files
- cluster and server reconciliation report a degraded condition if a Ranger profile is paired with datasets that still attach local `SecurityPolicy` objects

If you choose Ranger mode, your authorization rules should live in Ranger rather than on `Dataset.spec.securityPolicies`.

## Applying Security To Different Resource Types

### FusekiCluster

`FusekiCluster.spec.securityProfileRef` attaches a shared profile to the cluster's StatefulSet.

### FusekiServer

`FusekiServer.spec.securityProfileRef` applies the same style of security configuration to a standalone server deployment.

### Endpoint

`Endpoint.spec.securityProfileRef` lets published access points carry matching security context.

Example:

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: Endpoint
metadata:
  name: example-endpoint
  namespace: default
spec:
  targetRef:
    kind: FusekiCluster
    name: example
  securityProfileRef:
    name: local-security
```

### FusekiUI

`FusekiUI` targets an existing cluster or server.

The UI resource itself does not define a separate authentication model. It resolves the target and publishes a Service or optional ingress or gateway-facing resource for that target.

Minimal example:

```yaml
apiVersion: fuseki.apache.org/v1alpha1
kind: FusekiUI
metadata:
  name: ui1
  namespace: default
spec:
  targetRef:
    kind: FusekiCluster
    name: example
```

## Audit And Logging

### What Exists Today

The operator does not currently expose a dedicated audit CRD or an operator-managed audit pipeline.

Current audit-related reality:

- in `Local` mode, the operator manages authorization inputs, not a standalone audit backend
- in `Ranger` mode, audit behavior depends on your Ranger deployment and how Ranger itself is configured
- workload logging annotations exist under workload observability settings, but those are generic pod-logging annotations, not authorization audit controls

So if you need central audit trails today, the most realistic path is:

- run in `Ranger` mode
- use Ranger's own audit and policy visibility tooling
- collect container logs through your cluster logging stack as a supplement

### What Does Not Exist Yet

The following are not currently first-class operator features:

- audit sinks configured through a Fuseki operator CRD
- operator-managed retention or shipping for authz audit logs
- user-facing documentation for a production audit topology beyond Ranger integration

Be explicit about this in release messaging.

The operator has meaningful access-control features today, but its audit story is still externalized.

## Validation And Failure Modes

### Secret Validation

`SecurityProfile` reconciliation checks referenced secrets and reports `Pending` when references are missing or invalid.

Examples:

- missing `tls.crt` or `tls.key` on the TLS secret
- missing Ranger admin credentials secret
- missing `username` or `password` keys on the Ranger auth secret

### Fail-Closed Authorization

The runtime is configured with `authorization.failClosed=true`.

Consequences:

- unresolved local policy references block startup
- unsupported authorization modes block startup
- invalid Ranger configuration blocks startup

This is the right default for security-sensitive deployments, but it also means configuration mistakes are surfaced as readiness or startup failures rather than soft warnings.

## Troubleshooting

### Profile Stuck In `Pending`

Check:

- the referenced secrets exist in the same namespace
- TLS secrets contain `tls.crt` and `tls.key`
- Ranger auth secrets contain `username` and `password`

Useful commands:

```sh
kubectl get securityprofile -n default
kubectl describe securityprofile local-security -n default
kubectl get secret example-admin-secret -n default -o yaml
kubectl get secret example-tls-secret -n default -o yaml
```

### Dataset Or Cluster Refuses To Start In Local Mode

Check:

- all referenced `SecurityPolicy` objects exist
- the `Dataset.spec.securityPolicies` names are correct
- the cluster or server is not accidentally using a Ranger `SecurityProfile`
- if using `graphTagging`, at least one `actions` and one `subjects` entry is present on each tagging rule

### Ranger Mode Fails

Check:

- `authorization.mode` is set to `Ranger`
- `adminURL`, `serviceName`, and `authSecretRef` are set
- the Ranger admin endpoint is reachable from the workload
- you are not also attaching local `SecurityPolicy` bundles to the datasets used by that workload

## Recommended Alpha Usage

For alpha users, the safest patterns are:

- use `Local` mode when you want a self-contained cluster-level authorization setup driven by Kubernetes CRDs
- use `Ranger` mode when you already operate Apache Ranger and want centralized policy management and external audit visibility
- keep admin credentials explicit, even in dev clusters, so bootstrap jobs can create datasets deterministically
- use TLS in any shared or non-local environment
- treat audit as an external integration concern for now, not an operator-native subsystem
