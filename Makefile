GO ?= go
CONTAINER_TOOL ?= docker

-include images/fuseki/versions.mk

IMG ?= ghcr.io/example/fuseki-operator/controller:dev
FUSEKI_IMAGE ?= ghcr.io/example/fuseki-operator/fuseki:dev
RDF_DELTA_MOCK_IMAGE ?= ghcr.io/example/fuseki-operator/rdf-delta-mock:dev
JENA_VERSION ?=
JENA_SHA512 ?=

CONTROLLER_GEN = $(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen
SETUP_ENVTEST = $(GO) run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
ENVTEST_K8S_VERSION ?= 1.35.x

.PHONY: fmt vet test envtest e2e-k3d-m3 run generate manifests docker-build-fuseki docker-build-rdf-delta-mock docker-smoke-fuseki tidy

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

envtest:
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" $(GO) test ./internal/controller -run Envtest -count=1

e2e-k3d-m3:
	bash ./hack/e2e/k3d-m3.sh

run:
	$(GO) run ./cmd/manager

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

docker-build-rdf-delta-mock:
	$(CONTAINER_TOOL) build \
		-t $(RDF_DELTA_MOCK_IMAGE) \
		-f images/rdf-delta-mock/Dockerfile .

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
