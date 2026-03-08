GO ?= go
CONTAINER_TOOL ?= docker

-include images/fuseki/versions.mk
-include release/metadata.env

IMG ?= ${CONTROLLER_IMAGE}
JENA_VERSION ?=
JENA_SHA512 ?=
JENA_COMMANDS_SHA512 ?=

CONTROLLER_GEN = $(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.1
SETUP_ENVTEST = $(GO) run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
ENVTEST_K8S_VERSION ?= 1.35.x

.PHONY: fmt vet test envtest e2e-k3d-m3 e2e-k3d-m4-oidc e2e-k3d-m4-tls e2e-k3d-m5-backup-restore e2e-k3d-fusekiui-ingress run build-fusekictl run-fusekictl release-sync release-verify release-artifacts helm-lint helm-test bundle-refresh-crds bundle-validate bundle-build generate manifests docker-build-fuseki docker-build-rdf-delta docker-smoke-fuseki docker-smoke-rdf-delta tidy
.PHONY: docker-build-controller docker-smoke-controller

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test: manifests
	$(GO) test ./...

envtest: manifests
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" $(GO) test ./internal/controller -run Envtest -count=1

e2e-k3d-m3:
	bash ./hack/e2e/k3d-m3.sh

e2e-k3d-m4-oidc:
	bash ./hack/e2e/k3d-m4-oidc.sh

e2e-k3d-m4-tls:
	bash ./hack/e2e/k3d-m4-tls.sh

e2e-k3d-m5-backup-restore:
	bash ./hack/e2e/k3d-m5-backup-restore.sh

e2e-k3d-fusekiui-ingress:
	bash ./hack/e2e/k3d-m4-fusekiui-ingress.sh

run:
	$(GO) run ./cmd/manager

build-fusekictl:
	$(GO) build -o bin/fusekictl ./cmd/fusekictl

run-fusekictl:
	$(GO) run ./cmd/fusekictl $(ARGS)

release-sync:
	bash ./hack/release/sync-metadata.sh

release-verify:
	bash ./hack/release/verify-sync.sh

release-artifacts: release-sync bundle-refresh-crds
	bash ./hack/release/package-artifacts.sh

helm-lint: release-sync
	helm lint ./charts/fuseki-operator

helm-test: release-sync
	bash ./hack/helm/test-chart.sh

bundle-refresh-crds: manifests
	mkdir -p bundle/manifests
	cp ./config/crd/bases/*.yaml ./bundle/manifests/
	rm -f ./bundle/manifests/kustomization.yaml

bundle-validate: release-sync bundle-refresh-crds
	bash ./hack/olm/validate-bundle.sh

bundle-build: release-sync bundle-refresh-crds
	$(CONTAINER_TOOL) build -f bundle.Dockerfile -t $(BUNDLE_IMAGE) .

docker-build-controller:
	$(CONTAINER_TOOL) build \
		--build-arg VERSION=$(RELEASE_VERSION) \
		--build-arg COMMIT=$$(git rev-parse --short=12 HEAD 2>/dev/null || echo none) \
		--build-arg DATE=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		-t $(IMG) \
		-f images/controller/Dockerfile .

docker-smoke-controller: docker-build-controller
	set -eu; \
	output_file=$$(mktemp); \
	trap 'rm -f "'$$output_file'"' EXIT; \
	set +e; \
	$(CONTAINER_TOOL) run --rm --entrypoint /manager $(IMG) --help >"$$output_file" 2>&1; \
	exit_code=$$?; \
	set -e; \
	if [ $$exit_code -ne 0 ] && [ $$exit_code -ne 2 ]; then \
		cat "$$output_file"; \
		echo "controller image smoke test failed" >&2; \
		exit $$exit_code; \
	fi; \
	grep -q -- 'metrics-bind-address' "$$output_file" || { \
		cat "$$output_file"; \
		echo "controller image smoke test did not print manager help output" >&2; \
		exit 1; \
	}; \
	echo "Controller image smoke test passed"

tidy:
	$(GO) mod tidy

generate:
	$(CONTROLLER_GEN) object paths="./api/..."

manifests:
	mkdir -p config/crd/bases
	$(MAKE) generate
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

docker-build-fuseki:
	@test -n "$(JENA_VERSION)" || (echo "JENA_VERSION is required" >&2; exit 1)
	@test -n "$(JENA_SHA512)" || (echo "JENA_SHA512 is required" >&2; exit 1)
	$(CONTAINER_TOOL) build \
		--build-arg JENA_VERSION=$(JENA_VERSION) \
		--build-arg JENA_SHA512=$(JENA_SHA512) \
		-t $(FUSEKI_IMAGE) \
		-f images/fuseki/Dockerfile .


docker-build-rdf-delta:
	@test -n "$(JENA_VERSION)" || (echo "JENA_VERSION is required" >&2; exit 1)
	@test -n "$(JENA_COMMANDS_SHA512)" || (echo "JENA_COMMANDS_SHA512 is required" >&2; exit 1)
	$(CONTAINER_TOOL) build \
		--build-arg JENA_VERSION=$(JENA_VERSION) \
		--build-arg JENA_COMMANDS_SHA512=$(JENA_COMMANDS_SHA512) \
		-t $(RDF_DELTA_IMAGE) \
		-f images/rdf-delta/Dockerfile .

docker-smoke-fuseki: docker-build-fuseki
	container_id=$$($(CONTAINER_TOOL) run -d -p 13030:3030 $(FUSEKI_IMAGE)); \
	trap '$(CONTAINER_TOOL) rm -f '$$container_id' >/dev/null 2>&1 || true' EXIT; \
	for attempt in $$(seq 1 60); do \
		if curl --silent --fail 'http://127.0.0.1:13030/$$/ping' >/dev/null; then \
			echo "Fuseki image smoke test passed"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	$(CONTAINER_TOOL) logs $$container_id; \
	echo "Fuseki image smoke test failed" >&2; \
	exit 1

docker-smoke-rdf-delta: docker-build-rdf-delta
	set -eu; \
	container_id=$$($(CONTAINER_TOOL) run -d -p 13066:1066 $(RDF_DELTA_IMAGE) rdf-delta-server --port 1066 --storage /var/lib/rdf-delta); \
	trap '$(CONTAINER_TOOL) rm -f '$$container_id' >/dev/null 2>&1 || true' EXIT; \
	for attempt in $$(seq 1 60); do \
		if curl --silent --fail 'http://127.0.0.1:13066/$$/ping' >/dev/null; then \
			break; \
		fi; \
		sleep 2; \
		if [ $$attempt -eq 60 ]; then \
			$(CONTAINER_TOOL) logs $$container_id; \
			echo "RDF Delta image smoke test failed during startup" >&2; \
			exit 1; \
		fi; \
	done; \
	curl --silent --show-error --fail -X POST -H 'Content-Type: application/sparql-update' --data 'INSERT DATA { <urn:smoke:s> <urn:smoke:p> "ok" }' 'http://127.0.0.1:13066/delta/update' >/dev/null; \
	query_result=$$(curl --silent --show-error --fail --get --data-urlencode 'query=SELECT ?o WHERE { <urn:smoke:s> <urn:smoke:p> ?o }' -H 'Accept: text/csv' 'http://127.0.0.1:13066/delta/query'); \
	printf '%s\n' "$$query_result" | grep -q 'ok' || { \
		$(CONTAINER_TOOL) logs $$container_id; \
		echo "RDF Delta image smoke test failed during query validation" >&2; \
		exit 1; \
	}; \
	echo "RDF Delta image smoke test passed"
