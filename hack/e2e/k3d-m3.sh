#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-m3-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
FUSEKI_IMAGE="${FUSEKI_IMAGE:-ghcr.io/example/fuseki-operator/fuseki:e2e}"
RDF_DELTA_MOCK_IMAGE="${RDF_DELTA_MOCK_IMAGE:-ghcr.io/example/fuseki-operator/rdf-delta-mock:e2e}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-e2e-admin-password}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TMP_DIR="$(mktemp -d)"
MANAGER_LOG="${TMP_DIR}/manager.log"
PORT_FORWARD_LOG="${TMP_DIR}/port-forward.log"
METRICS_BIND_ADDRESS="${METRICS_BIND_ADDRESS:-127.0.0.1:0}"
PROBE_BIND_ADDRESS="${PROBE_BIND_ADDRESS:-127.0.0.1:0}"

print_section() {
	local title=$1
	shift
	echo >&2
	echo "===== ${title} =====" >&2
	"$@" >&2 || true
}

cluster_context_exists() {
	kubectl config get-contexts "${KUBECTL_CONTEXT}" >/dev/null 2>&1
}

namespace_exists() {
	kubectl --context "${KUBECTL_CONTEXT}" get namespace "${NAMESPACE}" >/dev/null 2>&1
}

dump_failure_diagnostics() {
	[[ "${DIAGNOSTICS_EMITTED:-0}" == "1" ]] && return 0
	DIAGNOSTICS_EMITTED=1

	echo >&2
	echo "e2e-k3d-m3 failed; collecting diagnostics" >&2

	if [[ -s "${MANAGER_LOG}" ]]; then
		print_section "manager log" cat "${MANAGER_LOG}"
	fi

	if ! cluster_context_exists; then
		echo "kubectl context ${KUBECTL_CONTEXT} is unavailable; skipping cluster diagnostics" >&2
		return 0
	fi

	if ! namespace_exists; then
		echo "namespace ${NAMESPACE} is unavailable; skipping namespace diagnostics" >&2
		return 0
	fi

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pods,sts,svc,jobs,leases,endpoints,cm
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "bootstrap job describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe job cluster-example-example-dataset-bootstrap
	print_section "bootstrap job logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs job/cluster-example-example-dataset-bootstrap
	print_section "fuseki statefulset describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe statefulset example
	print_section "rdf delta statefulset describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe statefulset example-delta

	for pod_name in example-0 example-1 example-delta-0; do
		if kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pod "${pod_name}" >/dev/null 2>&1; then
			print_section "describe pod/${pod_name}" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe pod "${pod_name}"
			print_section "logs pod/${pod_name}" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs "${pod_name}"
		fi
	done
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	if [[ -n "${PORT_FORWARD_PID:-}" ]]; then
		kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
	fi
	if [[ -n "${MANAGER_PID:-}" ]]; then
		kill "${MANAGER_PID}" >/dev/null 2>&1 || true
		wait "${MANAGER_PID}" >/dev/null 2>&1 || true
	fi
	if [[ "${KEEP_CLUSTER}" != "1" ]]; then
		k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
	fi
	rm -rf "${TMP_DIR}"
	return ${exit_code}
}
trap cleanup EXIT

require_tool() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "missing required tool: $1" >&2
		exit 1
	}
}

wait_for_http() {
	local url=$1
	for _ in $(seq 1 60); do
		if curl --silent --fail "$url" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done
	return 1
}

wait_for_manager() {
	for _ in $(seq 1 30); do
		if grep -q 'starting manager' "${MANAGER_LOG}" 2>/dev/null; then
			return 0
		fi
		sleep 1
	done
	echo "manager did not start successfully" >&2
		cat "${MANAGER_LOG}" >&2 || true
		return 1
}

require_tool docker
require_tool k3d
require_tool kubectl
require_tool curl

cd "${ROOT_DIR}"

make generate manifests
make docker-build-fuseki FUSEKI_IMAGE="${FUSEKI_IMAGE}"
make docker-build-rdf-delta-mock RDF_DELTA_MOCK_IMAGE="${RDF_DELTA_MOCK_IMAGE}"

k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
k3d image import -c "${CLUSTER_NAME}" "${FUSEKI_IMAGE}" "${RDF_DELTA_MOCK_IMAGE}"

kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null
kubectl apply -f config/crd/bases

go run ./cmd/manager --metrics-bind-address="${METRICS_BIND_ADDRESS}" --health-probe-bind-address="${PROBE_BIND_ADDRESS}" >"${MANAGER_LOG}" 2>&1 &
MANAGER_PID=$!
wait_for_manager

cat >"${TMP_DIR}/scenario.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
---
apiVersion: v1
kind: Secret
metadata:
  name: admin-secret
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  username: admin
  password: ${ADMIN_PASSWORD}
---
apiVersion: fuseki.apache.org/v1alpha1
kind: SecurityProfile
metadata:
  name: admin-auth
  namespace: ${NAMESPACE}
spec:
  adminCredentialsSecretRef:
    name: admin-secret
---
apiVersion: fuseki.apache.org/v1alpha1
kind: RDFDeltaServer
metadata:
  name: example-delta
  namespace: ${NAMESPACE}
spec:
  image: ${RDF_DELTA_MOCK_IMAGE}
  replicas: 1
  servicePort: 1066
  storage:
    accessMode: ReadWriteOnce
    size: 1Gi
---
apiVersion: fuseki.apache.org/v1alpha1
kind: Dataset
metadata:
  name: example-dataset
  namespace: ${NAMESPACE}
spec:
  name: primary
  type: TDB2
  spatial:
    enabled: true
    assembler: spatial:EntityMap
---
apiVersion: fuseki.apache.org/v1alpha1
kind: FusekiCluster
metadata:
  name: example
  namespace: ${NAMESPACE}
spec:
  replicas: 2
  image: ${FUSEKI_IMAGE}
  httpPort: 3030
  rdfDeltaServerRef:
    name: example-delta
  datasetRefs:
    - name: example-dataset
  securityProfileRef:
    name: admin-auth
  storage:
    accessMode: ReadWriteOnce
    size: 1Gi
EOF

kubectl apply -f "${TMP_DIR}/scenario.yaml"

kubectl rollout status statefulset/example-delta -n "${NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/example -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=complete job/cluster-example-example-dataset-bootstrap -n "${NAMESPACE}" --timeout=180s

lease_holder="$(kubectl get lease example-write -n "${NAMESPACE}" -o jsonpath='{.spec.holderIdentity}')"
if [[ -z "${lease_holder}" ]]; then
	echo "write lease holder was not selected" >&2
	exit 1
fi

write_endpoints="$(kubectl get endpoints example-write -n "${NAMESPACE}" -o jsonpath='{.subsets[*].addresses[*].ip}')"
read_endpoints="$(kubectl get endpoints example-read -n "${NAMESPACE}" -o jsonpath='{.subsets[*].addresses[*].ip}')"

if [[ "$(wc -w <<<"${write_endpoints}")" -ne 1 ]]; then
	echo "expected exactly one write endpoint, got: ${write_endpoints}" >&2
	exit 1
fi
if [[ "$(wc -w <<<"${read_endpoints}")" -lt 2 ]]; then
	echo "expected at least two read endpoints, got: ${read_endpoints}" >&2
	exit 1
fi

kubectl port-forward -n "${NAMESPACE}" service/example-write 13030:3030 >"${PORT_FORWARD_LOG}" 2>&1 &
PORT_FORWARD_PID=$!
wait_for_http 'http://127.0.0.1:13030/$/ping'

curl --silent --show-error --fail \
	-u "admin:${ADMIN_PASSWORD}" \
	-H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
	--data 'dbName=probe&dbType=tdb2' \
	'http://127.0.0.1:13030/$/datasets' >/dev/null

datasets_json="$(curl --silent --show-error --fail -u "admin:${ADMIN_PASSWORD}" -H 'Accept: application/json' 'http://127.0.0.1:13030/$/datasets')"

if ! grep -q 'primary' <<<"${datasets_json}"; then
	echo "expected bootstrapped dataset primary in Fuseki datasets response" >&2
	echo "${datasets_json}" >&2
	exit 1
fi
if ! grep -q 'probe' <<<"${datasets_json}"; then
	echo "expected probe dataset created through write service" >&2
	echo "${datasets_json}" >&2
	exit 1
fi

echo "M3 k3d e2e passed"
echo "Lease holder: ${lease_holder}"
echo "Write endpoints: ${write_endpoints}"
echo "Read endpoints: ${read_endpoints}"