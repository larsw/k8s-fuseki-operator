## Plan: v0.2.0 Execution Backlog

Ship v0.2.0 as a focused operator release for data governance and movement on top of the existing M3 to M5 platform. The delivery path is: first freeze APIs and validation, then build authorization and one-shot transfer primitives, then add continuous ingest and egress flows, then add autoscaling and release hardening. HeFQUIN federation and external management stay out of the release path and become design-only output.

**Milestones**
1. M0: API freeze and scaffolding. Add the missing API, validation, controller wiring, and shared status conventions needed by all feature work.
2. M1: Authorization foundation. Deliver dataset-level and named-graph authorization with explicit `Local` and `Ranger` modes, runtime wiring, and admission validation, including Apache Ranger-backed central policy enforcement for Fuseki servers and clusters.
3. M2: One-shot transfer operations. Deliver ImportRequest and ExportRequest with URL, S3, and filesystem support.
4. M3: Continuous pipelines. Deliver SHACL-gated ingress and RDF Delta-based change subscriptions.
5. M4: Autoscaling and runtime hardening. Deliver HPA support for FusekiCluster and verify routing and leader behavior under scale changes.
6. M5: Packaging, docs, and release validation. Finish samples, docs, Helm, OLM, tests, and release gates.
7. M6: Federation groundwork. Capture a future HeFQUIN-based FederationService design without shipping it in v0.2.0.

**Execution steps**
1. M0 blocks all feature milestones. Start by extending the API in /home/lars/code/k8s-fuseki-operator/api/v1alpha1/types.go and introducing any webhook or admission scaffolding required for cross-resource validation. Reuse the existing condition pattern from /home/lars/code/k8s-fuseki-operator/internal/controller/restorerequest_controller.go and /home/lars/code/k8s-fuseki-operator/internal/controller/rdfdeltaserver_controller.go so every new CRD follows the same status model.
2. M1 and M2 can begin in parallel once M0 lands. Authorization should use the Dataset securityPolicies linkage that already exists in /home/lars/code/k8s-fuseki-operator/api/v1alpha1/types.go and extend SecurityProfile or a closely related security API with an explicit authorization mode selector. In `Local` mode, operator-managed `SecurityPolicy` resources are allowed and enforced. In `Ranger` mode, local `SecurityPolicy` attachments are rejected by admission and centrally managed Ranger policies are enforced for dataset and named-graph access. Transfer operations should reuse request-style execution patterns from /home/lars/code/k8s-fuseki-operator/internal/controller/restorerequest_controller.go and helper patterns from /home/lars/code/k8s-fuseki-operator/internal/controller/backup_helpers.go.
3. M3 depends on the shared request and policy groundwork from M1 and M2. IngestPipeline should build on the import execution path and SHACLPolicy. ChangeSubscription should build on RDF Delta dependency resolution and durable status tracking in /home/lars/code/k8s-fuseki-operator/internal/controller/rdfdeltaserver_controller.go.
4. M4 can start once the M0 API freeze lands, but it should merge after M1 through M3 because its verification must include the new security and transfer flows. Autoscaling belongs in FusekiCluster reconciliation in /home/lars/code/k8s-fuseki-operator/internal/controller/fusekicluster_controller.go.
5. M5 depends on all shipping features. Regenerate CRDs, refresh samples, update Helm and OLM, and extend end-to-end coverage using the existing scripts under /home/lars/code/k8s-fuseki-operator/hack/e2e.
6. M6 is parallel with late M5 and is explicitly excluded from the v0.2.0 release gate.

**Issue backlog**
1. I0.1: Add v0.2.0 architecture note and API inventory.
Owner outcome: a short design note that defines SecurityPolicy, an explicit authorization mode model with `Local` and `Ranger` options, Apache Ranger integration settings, SHACLPolicy, ImportRequest, ExportRequest, IngestPipeline, ChangeSubscription, and FusekiCluster autoscaling fields, with explicit scope exclusions for federation and external lifecycle management.
Depends on: none.
Parallelism: none.
2. I0.2: Add admission and validation scaffolding.
Owner outcome: schema validation for all new fields plus admission-time validation for cross-resource checks that cannot be expressed in CRD schema alone, including rejection of mixed local-plus-Ranger authorization configuration.
Depends on: I0.1.
Parallelism: can run with I0.3 after field shapes are agreed.
3. I0.3: Add shared status and condition conventions.
Owner outcome: shared condition names, phase enums, and helper functions for request and pipeline resources aligned with existing restore and backup flows.
Depends on: I0.1.
Parallelism: can run with I0.2.
4. I0.4: Register new CRDs, RBAC, and controller manager wiring.
Owner outcome: controller stubs, scheme registration, and RBAC markers for all new resources.
Depends on: I0.1.
Parallelism: after field shapes are stable.
5. I1.1: Add SecurityPolicy API.
Owner outcome: a CRD that can target datasets and named graphs, express simple labels and Accumulo-style access expressions, and map subjects from OIDC claims or static identities for `Local` authorization mode. Add a companion authorization mode and Ranger integration model, most likely on SecurityProfile or a closely related security API, that points Fuseki workloads at Ranger-admin-managed policies for dataset and named-graph access in `Ranger` mode. Plan on supporting Accumulo expressions through an existing public parsing and evaluation library rather than a custom evaluator.
Depends on: I0.1 through I0.4.
Parallelism: can run with I2.1.
6. I1.2: Implement SecurityPolicy controller and dependency resolution.
Owner outcome: reconciliation that validates references, resolves Dataset and SecurityProfile dependencies, resolves Ranger admin connectivity and credentials, and renders consumable policy configuration for the selected authorization mode. In `Local` mode it enforces operator-managed `SecurityPolicy` resources. In `Ranger` mode it wires centrally managed Ranger policies and rejects local policy attachments.
Depends on: I1.1.
Parallelism: can run with I1.3.
7. I1.3: Add runtime authorization wiring.
Owner outcome: runtime config generation and image or mount changes in /home/lars/code/k8s-fuseki-operator/internal/controller/runtime_helpers.go and /home/lars/code/k8s-fuseki-operator/images/fuseki/Dockerfile so managed workloads enforce policy at request time for the selected authorization mode, using an existing public Maven package for Accumulo access-expression parsing and evaluation in `Local` mode and integrating with Apache Ranger for centrally managed policy enforcement in `Ranger` mode.
Depends on: I1.1.
Parallelism: can run with I1.2.
8. I1.4: Add security-focused tests and examples.
Owner outcome: unit and envtest coverage for invalid policy expressions, dataset and graph targeting, Ranger-backed central policy enforcement, admission rejection of mixed local-plus-Ranger configuration, fail-closed behavior for Ranger outages, and policy-driven runtime config plus sample manifests in /home/lars/code/k8s-fuseki-operator/config/samples.
Depends on: I1.2 and I1.3.
Parallelism: none.
9. I2.1: Add shared transfer execution helpers.
Owner outcome: job templating, source and sink validation, secret resolution, and condition updates for request-style data movement controllers.
Depends on: I0.3 and I0.4.
Parallelism: can run with I1.1.
10. I2.2: Add ImportRequest API and controller.
Owner outcome: one-shot imports from URL, S3, and filesystem into dataset or named graph targets, with explicit phase and error reporting.
Depends on: I2.1.
Parallelism: can run with I2.3 once helpers are ready.
11. I2.3: Add ExportRequest API and controller.
Owner outcome: one-shot exports from dataset or named graph targets to S3 and filesystem destinations, with durable status and artifact metadata.
Depends on: I2.1.
Parallelism: can run with I2.2.
12. I2.4: Add transfer credentials and secret reference validation.
Owner outcome: reusable validation and status surfacing for S3 credentials, local path constraints, and source reachability assumptions.
Depends on: I2.1.
Parallelism: can run with I2.2 and I2.3.
13. I2.5: Add one-shot transfer tests and samples.
Owner outcome: envtest coverage and sample manifests for import and export, including failing credential and target scenarios.
Depends on: I2.2 through I2.4.
Parallelism: none.
14. I3.1: Add SHACLPolicy API and validation.
Owner outcome: a CRD that stores SHACL bundles, rule references, failure handling options, and reporting controls.
Depends on: I0.1 through I0.4.
Parallelism: can run with I3.2 design work.
15. I3.2: Add IngestPipeline API and controller.
Owner outcome: a controller that ingests from supported sources, stages data, runs SHACL validation, and applies only passing payloads to the target dataset.
Depends on: I2.1 and I3.1.
Parallelism: can run with I3.3 after shared source contracts are stable.
16. I3.3: Add ChangeSubscription API and controller.
Owner outcome: a controller that consumes RDF Delta changes, tracks checkpoint or cursor state in status, and delivers to supported sinks.
Depends on: I0.3 and I2.1.
Parallelism: can run with I3.2.
17. I3.4: Add pipeline observability and failure artifact handling.
Owner outcome: consistent conditions, events, and retained summaries for SHACL failures, subscription lag, replay retries, and sink delivery failures.
Depends on: I3.2 and I3.3.
Parallelism: none.
18. I3.5: Add pipeline and subscription tests.
Owner outcome: envtest and k3d scenarios for SHACL pass and fail cases, ingress retries, RDF Delta subscription checkpointing, and failure recovery.
Depends on: I3.2 through I3.4.
Parallelism: none.
19. I4.1: Extend FusekiCluster API with autoscaling.
Owner outcome: autoscaling fields on FusekiClusterSpec for min replicas, max replicas, and metric targets, with defaults that preserve current cluster safety assumptions.
Depends on: I0.1 through I0.4.
Parallelism: can run with I3.2 and I3.3.
20. I4.2: Reconcile HorizontalPodAutoscaler resources.
Owner outcome: HPA creation and update in /home/lars/code/k8s-fuseki-operator/internal/controller/fusekicluster_controller.go, including safe behavior for leader-election and write-service routing.
Depends on: I4.1.
Parallelism: can run with I4.3.
21. I4.3: Validate scale behavior against read and write services.
Owner outcome: explicit tests and guardrails that confirm scaling events do not break the write endpoint, lease semantics, or replica read routing.
Depends on: I4.2.
Parallelism: none.
22. I4.4: Time-box custom metrics integration.
Owner outcome: either stable use of an existing metric path from WorkloadObservabilitySpec or an explicit defer decision recorded for a later release.
Depends on: I4.2.
Parallelism: can run with I4.3.
23. I5.1: Regenerate CRDs, samples, Helm, and OLM assets.
Owner outcome: all generated resources and package manifests reflect the new APIs and defaults.
Depends on: shipping feature APIs being stable.
Parallelism: can run with I5.2 after APIs stop moving.
24. I5.2: Update docs and release notes.
Owner outcome: user documentation for authorization, import, export, ingest pipelines, subscriptions, and autoscaling in /home/lars/code/k8s-fuseki-operator/README.md and /home/lars/code/k8s-fuseki-operator/docs.
Depends on: feature behavior being stable.
Parallelism: can run with I5.1.
25. I5.3: Add release-gate test coverage.
Owner outcome: end-to-end scripts under /home/lars/code/k8s-fuseki-operator/hack/e2e plus unit and envtest coverage sufficient for v0.2.0 sign-off.
Depends on: I1.4, I2.5, I3.5, and I4.3.
Parallelism: limited.
26. I5.4: Run packaging and release verification.
Owner outcome: passing execution of test, envtest, helm-test, release-sync, release-verify, bundle-refresh-crds, and bundle-validate from /home/lars/code/k8s-fuseki-operator/Makefile.
Depends on: I5.1 through I5.3.
Parallelism: none.
27. I6.1: Write HeFQUIN federation design note.
Owner outcome: a design-only proposal for a future FederationService resource that composes remote SPARQL endpoints with in-cluster datasets and the new SecurityPolicy model.
Depends on: I0.1 and enough clarity from I1.1.
Parallelism: can run late in parallel with I5.2 and I5.3.

**Suggested implementation order**
1. Week 1 to 2: I0.1 through I0.4, then open parallel tracks for I1.1 and I2.1.
2. Week 2 to 4: finish I1.2 through I1.4 and I2.2 through I2.5.
3. Week 4 to 6: deliver I3.1 through I3.5.
4. Week 5 to 6: deliver I4.1 through I4.4 in parallel with late M3 testing.
5. Week 6 to 7: deliver I5.1 through I5.4 and I6.1.

**Relevant files**
- /home/lars/code/k8s-fuseki-operator/api/v1alpha1/types.go — current API entry point for FusekiCluster, Dataset, SecurityProfile, BackupPolicy, RestoreRequest, and future v0.2.0 resources.
- /home/lars/code/k8s-fuseki-operator/internal/controller/fusekicluster_controller.go — autoscaling and service-routing behavior.
- /home/lars/code/k8s-fuseki-operator/internal/controller/dataset_controller.go — dataset materialization and import pipeline base patterns.
- /home/lars/code/k8s-fuseki-operator/internal/controller/rdfdeltaserver_controller.go — RDF Delta dependency and status patterns for subscriptions.
- /home/lars/code/k8s-fuseki-operator/internal/controller/restorerequest_controller.go — request-style phase machine template.
- /home/lars/code/k8s-fuseki-operator/internal/controller/securityprofile_controller.go — authentication integration point.
- /home/lars/code/k8s-fuseki-operator/internal/controller/backup_helpers.go — secret and job helper patterns.
- /home/lars/code/k8s-fuseki-operator/internal/controller/runtime_helpers.go — runtime config and mount wiring.
- /home/lars/code/k8s-fuseki-operator/images/fuseki/Dockerfile — minimal runtime extensions needed by security and validation features.
- /home/lars/code/k8s-fuseki-operator/config/samples — new sample manifests.
- /home/lars/code/k8s-fuseki-operator/hack/e2e — release-gate scenarios.
- /home/lars/code/k8s-fuseki-operator/charts/fuseki-operator and /home/lars/code/k8s-fuseki-operator/bundle/manifests — packaging updates.
- /home/lars/code/k8s-fuseki-operator/Makefile — release gate commands.

**Verification**
1. M0 exit: new APIs compile, generate clean CRDs, register with the manager, and enforce baseline schema and admission validation.
2. M1 exit: `Local` mode works for dataset and named-graph authorization with failing policy definitions rejected early, `Ranger` mode enforces centrally managed Apache Ranger policies in Fuseki servers and clusters, mixed mode usage is rejected by admission, and runtime enforcement is visible in managed workloads.
3. M2 exit: one-shot imports and exports succeed for supported source and sink types and fail with actionable status when dependencies are invalid.
4. M3 exit: SHACL-gated ingest rejects invalid payloads and accepts valid ones, and RDF Delta subscriptions resume from stored checkpoints after restarts.
5. M4 exit: HPA scale-out and scale-in do not break write routing, readiness, or lease-driven write leadership.
6. M5 exit: tests, envtest, helm-test, release-sync, release-verify, bundle-refresh-crds, and bundle-validate pass.

**Decisions**
- Included in v0.2.0: SecurityPolicy, Apache Ranger integration for central policy enforcement, SHACLPolicy, ImportRequest, ExportRequest, IngestPipeline, ChangeSubscription, and FusekiCluster autoscaling.
- Excluded from v0.2.0 shipping scope: FederationService, external cluster lifecycle management, and event-driven scaling beyond standard HPA.
- Security is enforced at both admission time and runtime through explicit authorization modes. `Local` mode uses operator-managed `SecurityPolicy` resources. `Ranger` mode uses centrally managed Apache Ranger policies. Mixed local-plus-Ranger policy composition is not supported in v0.2.0.
- Import and export prioritize on-demand requests; continuous data movement is handled by separate long-lived resources.
- S3 and filesystem are the only required sink types for v0.2.0. URL, S3, and filesystem are the required source types.

**Further considerations**
1. Use an existing public Maven package for Accumulo access-expression parsing and evaluation rather than building a custom evaluator. Time-box only the runtime integration and policy-mapping work; if that still proves unstable, keep the full API surface and declare any temporary evaluator limits in the v0.2.0 release notes.
2. Keep the alpha API strict: exactly one authorization mode per deployment. In `Ranger` mode, reject local `SecurityPolicy` attachments by admission rather than trying to merge or override policies.
3. Default to fail-closed behavior on Ranger connectivity loss and surface explicit degraded status conditions. If a later release needs configurable fail-open behavior, add it explicitly rather than implying it through runtime fallback.
4. If webhook scaffolding proves too invasive for the current manager setup, fall back to CRD schema plus reconcile-time invalid status only as a temporary branch, but treat true admission enforcement as the release target.
5. Keep FederationService out of the public API until the security and transfer model stabilizes on the shipped resources.
