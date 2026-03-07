## Plan: Fuseki Operator and CLI

Build a Go-based Kubernetes operator (Kubebuilder/Operator SDK style) with CRDs to manage Apache Jena Fuseki clusters using RDF Delta for HA scaling, plus a Go fusekictl CLI for install/ops/observe and a project-owned Fuseki container image derived from the stain/jena-docker approach. Target K8s 1.33+, Helm/OLM bundles, semantic versioning.

**Steps**
1. Bootstrap project: initialize Go module; scaffold operator layout (Kubebuilder); enable admission webhooks; set up CI skeleton (lint, unit, envtest, k3d e2e placeholders).
2. Custom Fuseki image build: create a project-owned Docker build based on the stain/jena-docker Fuseki layout and helper-script pattern, but maintained in-repo; parameterize the Jena/Fuseki version and checksum via build args or release-time env vars rather than hard-coding in the Dockerfile; preserve support for entrypoint initialization, dataset bootstrapping, extra classpath JARs, JVM tuning, and future Jena Spatial plugin injection; wire image build/publish into CI and Helm values.
3. CRD design (API group e.g., delta.fuseki.apache.org, version v1alpha1):
   - FusekiCluster: size, image, resources, storage class/size, leader election settings, RDF Delta config (patch log service ref), services (write/read), PDB, scaling rules, GeoSPARQL toggle (default off), feature gates.
   - RDFDeltaServer: single patch log service (StatefulSet) with PVC, optional replicas/leader election strategy, backup policy reference, retention, S3/GCS credentials, TLS.
   - Dataset: dataset type (TDB2/others), persistence (PVC or ephemeral), preload sources, backups, Jena Spatial enable flag and declarative spatial configuration.
   - Endpoint: HTTP endpoints (query/update), authn/authz requirements, rate limiting, exposure (ClusterIP/LoadBalancer/Ingress), CORS.
   - SecurityProfile: TLS config, OIDC/SAML IdP refs, RBAC-style roles, secrets references for admin/user creds.
   - BackupPolicy: schedule, target (S3-compatible object store first, then GCS/PVC later), retention, encryption, restore action CR.
4. Controller implementation:
   - RDFDeltaServer controller: reconciles StatefulSet + PVC + Service; optional backup CronJob per BackupPolicy; health checks; config Secret/ConfigMap generation; supports manual or automatic leader election if replicas>1 (K8s Lease per server).
   - FusekiCluster controller: reconciles StatefulSet of Fuseki pods; sidecar/embedded leader elector using K8s Lease to pick master; labels/Service for master vs replicas; uses RWO storage on the current master while replicas rebuild and follow through RDF Delta logs; init with RDF Delta config from RDFDeltaServer; PDB; rollout strategy; config regeneration on spec changes.
   - Dataset controller: ensures datasets exist on master (job/sidecar init); manages PVCs; propagates RDF Delta registration; applies declarative Jena Spatial configuration only for datasets that opt in; optional restore from BackupPolicy.
   - Endpoint controller: wires Services/Ingress; integrates SecurityProfile; sets authz rules; configures rate limits via annotations/sidecar; creates separate read and write Services so mutating operations always target the current lease holder; optionally emits Gateway API or ingress rules that route known write paths and mutating methods to the write Service when the chosen ingress implementation supports method matching.
   - SecurityProfile controller: manages Secrets/ConfigMaps for TLS/OIDC/SAML; distributes to dependent resources.
   - Observability hooks: metrics Service + ServiceMonitor, log forwarding annotations, optional OTLP exporter sidecar.
5. CLI fusekictl (Go): commands install/uninstall CRDs and operator (Helm/OLM); create/delete clusters/datasets/endpoints; status/health; trigger backups/restores; tail logs; kubeconfig discovery; JSON/YAML output; completions.
6. Packaging and releases: Helm chart for operator and CRDs; OLM bundle generation; semantic version tagging; container images build/push; sample manifests; docs.
7. Testing and validation: unit tests; envtest for controllers; k3d e2e pipeline exercising cluster creation, leader failover, backup/restore, security/OIDC smoke; helm lint/chart tests; operator-sdk bundle validate.

**Relevant files**
- Go module: go.mod, go.sum.
- Operator scaffolding: config/* (CRDs, kustomize, webhook), controllers/*, api/* for types.
- Custom image: images/fuseki/Dockerfile, images/fuseki/docker-entrypoint.sh, images/fuseki/load.sh, images/fuseki/tdbloader, images/fuseki/tdbloader2, images/fuseki/shiro.ini, images/fuseki/versions.mk or equivalent build metadata.
- CLI: cmd/fusekictl/main.go, pkg/cli/*, pkg/kube/*.
- Charts/bundles: charts/fuseki-operator/*, bundle/*.
- CI: .github/workflows/ci.yaml (lint/unit/e2e/build/release/image).
- Docs/examples: docs/*, examples/*.

**Verification**
1. go test ./...
2. envtest for controllers covering reconcile flows and webhook validation.
3. Container validation: docker build or docker buildx build for the project-owned Fuseki image with explicit Jena/Fuseki version and checksum args; smoke-test startup, admin bootstrap, dataset creation, helper scripts, and optional extra JAR mounting.
4. k3d e2e: deploy operator via Helm, create RDFDeltaServer + FusekiCluster + Dataset + Endpoint; verify write-master election, read replicas, failover; execute SPARQL queries; verify mutating requests go only through the write Service; test backup/restore to S3-compatible storage.
5. helm lint and chart test; operator-sdk bundle validate; kube-score or kubelinter for manifests.
6. fusekictl smoke: install/uninstall, status, backup trigger, log tail against k3d cluster.

**Decisions**
- Language Go; target K8s 1.33+; packaging via Helm and OLM bundle; semantic versioning.
- Maintain a project-owned Fuseki image in-repo, based on the stain/jena-docker structure and scripts, rather than consuming the upstream image directly.
- Parameterize Jena/Fuseki version and release checksum at build time; do not hard-code release versions in the Dockerfile. CI and release automation should inject the current target version and checksum for reproducible builds.
- RDF Delta topology: single patch log service CRD with optional HA/leader election and optional backups via BackupPolicy.
- Security: include TLS, OIDC/SAML, RBAC-like roles; no multi-tenancy.
- Storage: use the preferred RWO mode where the current master owns the writable dataset PVC and replicas follow through RDF Delta logs; failover via K8s Lease-based leader election per cluster.
- Request routing: expose separate write and read Services. All mutating operations route to the write Service, which selects only the current lease holder; read/query traffic routes to the read Service. Where supported, generate Gateway API or ingress rules for method and path-based routing; otherwise clients and fusekictl use distinct read/write endpoints explicitly.
- Backups: implement S3-compatible object storage first via BackupPolicy.
- GeoSPARQL: use Jena Spatial as an opt-in feature per Dataset via declarative CRD config and image plugin hooks.
- Observability: metrics plus logs plus optional traces (Prometheus/Grafana, OpenTelemetry).
- Testing: e2e on k3d required.

**Further Considerations**
1. Mutating HTTP verbs and administrative write operations should go to a dedicated write endpoint backed by a Service that selects only the current lease-holder pod. This avoids depending on ingress implementations for method matching and gives fusekictl a stable write target.
2. If a cluster ingress or Gateway API supports method and path matching, the operator can optionally publish a single external hostname that routes GET and HEAD query traffic to the read Service and POST, PUT, PATCH, and DELETE write paths to the write Service.
3. Read traffic should default to a separate read endpoint backed by replica pods, with optional fallback to the master when no replicas are ready.
4. Future backup targets can extend BackupPolicy to GCS or PVC snapshots after the S3-compatible implementation is stable.

**Implementation Backlog**

**Milestones**
1. M0: Repository foundation
   Deliverables: Go module initialization, Kubebuilder project scaffold, base Makefile/tasks, lint/test toolchain, CI skeleton, developer docs, local k3d workflow.
   Exit criteria: `make test`, lint, and scaffold generation run locally and in CI.
2. M1: Custom Fuseki image
   Deliverables: in-repo Fuseki image build based on the stain/jena-docker pattern, parameterized Jena/Fuseki version and checksum inputs, helper scripts, startup and healthcheck smoke tests, published image workflow.
   Exit criteria: image builds reproducibly from explicit version inputs and starts successfully in local smoke tests.
3. M2: API surface and CRDs
   Deliverables: v1alpha1 API types, CRD manifests, validation/defaulting webhooks where needed, sample manifests, API reference docs.
   Exit criteria: CRDs install cleanly, schema validation works, and sample resources pass admission checks.
4. M3: Core data-plane reconciliation
   Deliverables: RDFDeltaServer, FusekiCluster, and Dataset controllers; StatefulSets, Services, PVCs, leader election, master/write routing, dataset bootstrap, Jena Spatial dataset hooks.
   Exit criteria: a cluster can be created in k3d, elect a master, serve read traffic, and accept writes through the write endpoint.
5. M4: Platform integrations
   Deliverables: Endpoint and SecurityProfile controllers, TLS integration, OIDC/SAML wiring, ServiceMonitor support, structured logs, optional OTLP exporter integration.
   Exit criteria: secured endpoints and observability integrations work end-to-end in example deployments.
6. M5: Backup and restore
   Deliverables: BackupPolicy and restore workflows, S3-compatible backup jobs, retention handling, restore orchestration, operational status conditions.
   Exit criteria: scheduled and ad hoc backups complete successfully and a restore can rebuild a working cluster.
7. M6: CLI and release packaging
   Deliverables: `fusekictl` install/uninstall/status/backup commands, Helm chart, OLM bundle, release automation, versioned docs, upgrade notes.
   Exit criteria: a user can install, observe, operate, and remove the operator and clusters using documented commands.
8. M7: Hardening and release readiness
   Deliverables: failure-mode tests, upgrade tests, performance baseline, security review, conformance docs, semver release checklist.
   Exit criteria: k3d e2e suite is stable, release artifacts are generated, and the first tagged prerelease is publishable.

**Epics**
1. E1: Developer platform and scaffolding
   Scope: repository layout, code generation, Make targets, CI jobs, test harnesses, docs for contributors.
   Dependencies: none.
   Priority: immediate.
2. E2: Project-owned Fuseki runtime image
   Scope: Dockerfile, entrypoint, helper scripts, plugin injection path, version/checksum parameterization, image publishing.
   Dependencies: E1.
   Priority: immediate.
3. E3: Core API and CRD model
   Scope: FusekiCluster, RDFDeltaServer, Dataset, Endpoint, SecurityProfile, BackupPolicy, shared status conditions, references, defaults.
   Dependencies: E1.
   Priority: immediate.
4. E4: RDF Delta and Fuseki reconciliation
   Scope: StatefulSets, Services, PVCs, lease-based leader election, master/read service split, config rendering, rolling updates, failover behavior.
   Dependencies: E2, E3.
   Priority: high.
5. E5: Dataset lifecycle and spatial features
   Scope: dataset bootstrap, preload jobs, restore hooks, Jena Spatial opt-in configuration, dataset status and readiness.
   Dependencies: E2, E3, E4.
   Priority: high.
6. E6: Security and exposure model
   Scope: TLS, OIDC/SAML configuration distribution, authn/authz wiring, ingress or Gateway API exposure, rate limiting integration.
   Dependencies: E3, E4.
   Priority: medium.
7. E7: Backups and disaster recovery
   Scope: S3-compatible backup execution, retention policies, restore workflow, failure reporting, credentials handling.
   Dependencies: E3, E4, E5.
   Priority: high.
8. E8: Observability and operability
   Scope: metrics, logs, traces, conditions, events, admin commands, runbooks, health dashboards.
   Dependencies: E4.
   Priority: medium.
9. E9: `fusekictl` user workflow
   Scope: install/uninstall, CRUD helpers, status, logs, backup and restore triggers, shell completion, machine-readable output.
   Dependencies: E3, E4, E7.
   Priority: high.
10. E10: Packaging, upgrades, and release engineering
   Scope: Helm chart, OLM bundle, image tagging, semver release process, upgrade docs, compatibility matrix.
   Dependencies: E2, E3, E4, E9.
   Priority: medium.

**Initial Sprint Order**
1. Sprint 1: E1 plus the minimum viable parts of E2 and E3 needed to generate CRDs and build the custom image locally.
2. Sprint 2: E4 for RDFDeltaServer and FusekiCluster reconciliation with lease-based master election and read/write Services.
3. Sprint 3: E5 for dataset bootstrap and Jena Spatial opt-in support, plus the first k3d end-to-end test path.
4. Sprint 4: E7 for S3-compatible backups and restores, plus E9 status and backup CLI commands.
5. Sprint 5: E6 and E8 for security, exposure, metrics, logs, and traces.
6. Sprint 6: E10 for packaging, upgrades, and prerelease hardening.

**Definition Of Done**
1. Every CRD field added to the API has schema validation, status reporting, and an example manifest.
2. Every controller has unit coverage for reconciliation branches and envtest coverage for API interactions.
3. Every milestone adds at least one k3d end-to-end scenario covering the user-visible behavior introduced in that milestone.
4. Every externally exposed feature is reachable through both YAML manifests and `fusekictl` when applicable.
5. Every release artifact includes versioned docs, image tags, and upgrade notes.

**M0 And M1 Breakdown**

**M0: Repository Foundation Tasks**
1. Initialize the Go module and choose a temporary local-safe module path that can be renamed later if the final git remote changes.
2. Create a controller-runtime based manager entrypoint and repository layout compatible with later Kubebuilder-style generation.
3. Add base packages for APIs, controllers, version metadata, and shared configuration.
4. Add Make targets for formatting, vetting, testing, manifest generation, local runs, and custom image builds.
5. Add toolchain documentation for Go, controller-gen, kustomize, Docker, and k3d.
6. Add CI skeleton jobs for Go test, lint or vet, and custom image build validation.
7. Add a local development guide covering bootstrap, test, and future k3d flows.

**M1: Custom Fuseki Image Tasks**
1. Create `images/fuseki/` with a parameterized Dockerfile requiring explicit Jena version and checksum inputs.
2. Add startup and helper scripts for Fuseki initialization, dataset bootstrap, and loader wrappers.
3. Add a base `shiro.ini` template and plugin extension path for future Jena Spatial support.
4. Add a version-input example file or Make include file that documents required build arguments without hard-coding release values in the Dockerfile.
5. Add a `make docker-build-fuseki` target and CI stub that validates the image can be built when version inputs are provided.
6. Reserve image hooks and directory conventions for extra JARs, dataset initialization, and future GeoSPARQL enablement.

**Scaffold Acceptance For This First Pass**
1. The repository should build and run basic Go tests even before any controllers are implemented.
2. The manager binary should start with health and readiness probes and leader election flags.
3. The custom Fuseki image scaffold should exist and document the required build inputs, even if release automation is not yet wired.
4. The repository layout should not block later adoption of full Kubebuilder generation and controller scaffolding.
