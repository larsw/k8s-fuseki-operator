## Plan: OPA as Alternative Authorization Mode for Named Graphs

Add `OPA` as a third `AuthorizationMode` (alongside `Local` and `Ranger`) in the `SecurityProfile` CRD, backed by a new `FusekiOpaAuthorizationFilter` servlet filter that calls an OPA sidecar or standalone service for per-request authorization decisions on datasets and named graphs.

The current operator wires authorization via `spec.authorization.mode` on `SecurityProfile`, with each mode backed by a dedicated Jakarta servlet filter in the Fuseki image. OPA plugs into this same pattern: new enum value → new config struct → operator renders env vars / injects sidecar → filter calls OPA at request time.

**Recommended architecture: OPA sidecar** injected by the operator into Fuseki pods (localhost calls, ~1-2ms latency, per-pod blast radius). A `url` field allows pointing at a shared external OPA instance instead.

---

### Phase 1: API Extension

1. Add `OPA` to `AuthorizationMode` enum in [api/v1alpha1/v020_types.go](api/v1alpha1/v020_types.go)
2. Add `OpaAuthorizationSpec` struct with fields: `url` (optional, defaults to sidecar at `localhost:8181`), `policyPackage` (required, e.g. `fuseki.authz`), `bundleURL` (optional), `bundleSecretRef` (optional, key: `token`), `image` (optional, default OPA image), `cacheTTL` (optional, default 5s), `failClosed` (optional, default true), `decisionLog` (optional struct: `enabled` bool, `console` bool, `remoteURL` string, `remoteSecretRef` *LocalObjectReference)
3. Add `OPA *OpaAuthorizationSpec` to `SecurityAuthorizationSpec` with CEL rules enforcing mutual exclusivity (same pattern as Ranger)
4. `make generate manifests` to regenerate deepcopy + CRD YAML
5. Add sample [config/samples/fuseki_v1alpha1_securityprofile_opa.yaml](config/samples/fuseki_v1alpha1_securityprofile_opa.yaml)

### Phase 2: Operator Wiring

6. Extend `renderSecurityProfileConfigData()` in [runtime_helpers.go](internal/controller/runtime_helpers.go) to emit OPA properties
7. Extend `fusekiSecurityEnvVars()` to inject `SECURITY_PROFILE_OPA_*` env vars
8. Extend [securityprofile_controller.go](internal/controller/securityprofile_controller.go) to validate `bundleSecretRef`
9. Extend admission in [v020_validation.go](internal/controller/v020_validation.go) — OPA mode rejects Ranger config and local `SecurityPolicy` attachments
10. **Inject OPA sidecar container** when `url` is omitted — append OPA container to Fuseki pod template with `opa run --server --bundle=...` args, or mount policy ConfigMap if no bundle URL. Generate `--decision-log-console-stdout` / `--decision-log-plugin-url` args from `decisionLog` config when enabled
11. Extend `resolveFusekiWorkloadSecurityDependency()` in [security_helpers.go](internal/controller/security_helpers.go) with same no-mixed-mode check as Ranger

### Phase 3: Runtime Filter (Java)

12. **Extract shared request model** — move `RequestTarget`, `RequestPrincipal`, `RequestAction` from [FusekiAuthorizationFilter.java](images/fuseki/src/main/java/FusekiAuthorizationFilter.java) into `FusekiAuthorizationRequest.java`. Refactor Local and Ranger filters to import from it
13. **Create `FusekiOpaAuthorizationFilter.java`** (imports shared request model) — on each request, build an OPA input document from request context, POST to `{opaURL}/v1/data/{policyPackage}`, parse OPA response, cache decisions by (user, action, dataset, namedGraphs, graphsResolved) with TTL, fail-closed on OPA errors. Two code paths: (A) when graphs are resolved from the request (REST graph-store, explicit `FROM NAMED`/`GRAPH <iri>`), set `graphsResolved: true` and send the extracted IRIs — OPA returns allow/deny; (B) when graphs are unresolved (variable `GRAPH ?g`, no graph clause), set `graphsResolved: false` — OPA returns `allowedGraphs`, and the filter rewrites the SPARQL query with `VALUES` constraints to restrict execution to authorized graphs only
14. Wire in [FusekiHttpsLauncher.java](images/fuseki/src/main/java/FusekiHttpsLauncher.java) — add `case "OPA"` to filter selection

**OPA input document:**
```json
{
  "input": {
    "user": "alice",
    "groups": ["analysts"],
    "roles": ["viewer"],
    "oidcClaims": {"department": "research"},
    "action": "Query",
    "dataset": "knowledge-graph",
    "namedGraphs": ["https://example.com/graphs/public"],
    "graphsResolved": true,
    "endpoint": "query",
    "path": "/knowledge-graph/query"
  }
}
```

**Named graph resolution strategy:**

Not all SPARQL requests identify named graphs upfront. The filter uses a two-tier model:

1. **Graphs known at request time** (`graphsResolved: true`) — REST graph-store requests (`?graph=<iri>`), queries with explicit `FROM NAMED` / `GRAPH <iri>` clauses. The filter extracts the IRIs, populates `namedGraphs[]`, and OPA decides per-graph. This is the simple path.

2. **Graphs not known at request time** (`graphsResolved: false`) — queries with variable graph patterns (`GRAPH ?g { ... }`), or no graph clause at all (default graph). The filter cannot enumerate graphs before execution. Instead it asks OPA for an **allowed graph set**: the OPA response includes `allowedGraphs` (an array of IRIs or patterns the principal may access). The filter injects these as a `VALUES ?g { ... }` constraint or equivalent SPARQL rewrite to restrict execution to authorized graphs only. If OPA returns an empty allowed set, the query is denied.

**OPA response schema (extended):**
```json
{
  "result": {
    "allow": true,
    "allowedGraphs": [
      "https://example.com/graphs/public",
      "https://example.com/graphs/shared/*"
    ]
  }
}
```

When `graphsResolved` is true, OPA only needs to return `allow`. When false, OPA returns `allowedGraphs` so the filter can constrain the query. Wildcard/glob entries in `allowedGraphs` are expanded by the filter against the dataset's known graph list before injection.

### Phase 4: Reference Policy & Docs

15. Create reference Rego policy in `hack/opa/` — `fuseki/authz.rego` with named-graph rules including wildcard and regex matching on graph IRIs (e.g. `glob.match("https://example.com/graphs/public/*", ...)` for prefix/wildcard patterns, `regex.match("^https://example\\.com/graphs/(dev|staging)/.*$", ...)` for regex) + `fuseki/authz_test.rego` covering exact, wildcard, and regex graph IRI matches as well as explicit deny rules
16. Update [docs/accesscontrol.md](docs/accesscontrol.md) with OPA section (config, input schema, writing Rego policies for named-graph wildcard/regex matching, sidecar vs. standalone)

---

### Relevant Files

- [api/v1alpha1/v020_types.go](api/v1alpha1/v020_types.go) — `OpaAuthorizationSpec`, mode enum, CEL rules
- [api/v1alpha1/types.go](api/v1alpha1/types.go) — `SecurityAuthorizationSpec.OPA` field
- [internal/controller/runtime_helpers.go](internal/controller/runtime_helpers.go) — `renderSecurityProfileConfigData()`, `fusekiSecurityEnvVars()`, sidecar injection
- [internal/controller/security_helpers.go](internal/controller/security_helpers.go) — mixed-mode rejection
- [internal/controller/securityprofile_controller.go](internal/controller/securityprofile_controller.go) — bundle secret validation
- [internal/controller/v020_validation.go](internal/controller/v020_validation.go) — admission rules
- [images/fuseki/src/main/java/FusekiAuthorizationFilter.java](images/fuseki/src/main/java/FusekiAuthorizationFilter.java) — extract shared request model
- `images/fuseki/src/main/java/FusekiAuthorizationRequest.java` — new file: shared request model
- `images/fuseki/src/main/java/FusekiOpaAuthorizationFilter.java` — new file
- [images/fuseki/src/main/java/FusekiHttpsLauncher.java](images/fuseki/src/main/java/FusekiHttpsLauncher.java) — add OPA case

### Verification

1. `make generate manifests` succeeds — CRD includes OPA fields
2. Unit tests for OPA config rendering (env vars, security.properties)
3. Unit tests for admission: OPA rejects Ranger config, requires OPA config, rejects local SecurityPolicy
4. Java tests for `FusekiOpaAuthorizationFilter`: mock OPA responses, verify allow/deny/cache/failClosed
5. Java tests for shared `FusekiAuthorizationRequest`: existing Local and Ranger filter tests still pass after extraction
6. `opa test` passes on reference Rego policy
7. Smoke test: deploy Fuseki + OPA sidecar, apply reference policy, verify named-graph authz

### Decisions

- **OPA + local SecurityPolicy: rejected** (same as Ranger) — OPA owns all policy in Rego; no dual enforcement
- **Sidecar default, standalone optional** — omit `url` to get auto-injected sidecar; provide `url` for external OPA
- **Fail-closed default** — matches existing convention
- **No Accumulo expression bridging** — Rego is OPA's native language; users implement expression logic there
- **RDF* graph tagging is supported in OPA mode** — OPA evaluates from request context + its own data AND reading RDF* annotations at request time - if available.
- **Data push + pull** — Both supported. Operator renders graph ACL data into a ConfigMap the sidecar mounts (push); `bundleURL` field points OPA at an external bundle server (pull). Users pick whichever fits their workflow.
- **Decision logging included** — Wire OPA `--decision-log-*` flags from day one. Add `decisionLog` fields to `OpaAuthorizationSpec` (`enabled` bool, `console` bool for stderr output, `remoteURL` + `remoteSecretRef` for remote sink). Sidecar args generated accordingly.
- **Shared request model extraction** — Extract `RequestTarget`, `RequestPrincipal`, `RequestAction` into `FusekiAuthorizationRequest.java`. Refactor Local and Ranger filters to import from it. OPA filter uses the same shared classes. Prevents drift across three filters.
- **Wildcard/regex matching on named graph IRIs** — The reference Rego policy demonstrates three matching modes: exact IRI match, glob/wildcard patterns (via Rego `glob.match`), and full regex (via Rego `regex.match`). Policy authors use whichever granularity they need. Deny rules can use the same matching — e.g. deny all graphs under `https://example.com/graphs/internal/*` for external users. This is a Rego-native capability requiring no operator-side changes; it is documented and showcased in the reference policy and `accesscontrol.md`.
- **Two-tier named graph resolution** — SPARQL queries don't always identify target graphs upfront (`GRAPH ?g` patterns, default graph). The filter distinguishes resolved vs. unresolved graphs via `graphsResolved` in the OPA input. When resolved, OPA returns a simple allow/deny. When unresolved, OPA returns an `allowedGraphs` set and the filter rewrites the SPARQL query with `VALUES` constraints to restrict variable graph patterns to authorized IRIs only. Glob entries in `allowedGraphs` are expanded against the dataset's known graph list before injection.
