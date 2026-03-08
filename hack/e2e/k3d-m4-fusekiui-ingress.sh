#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-ui-ingress-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-ui-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
FUSEKI_IMAGE="${FUSEKI_IMAGE:-ghcr.io/larsw/k8s-fuseki-operator/fuseki:e2e}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-e2e-admin-password}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TMP_DIR="$(mktemp -d)"
MANAGER_LOG="${TMP_DIR}/manager.log"
INGRESS_FORWARD_LOG="${TMP_DIR}/ingress-port-forward.log"
UI_RESPONSE_FILE="${TMP_DIR}/ui-response.html"
INGRESS_HOST="${INGRESS_HOST:-fuseki.example.test}"
INGRESS_CLASS_NAME="${INGRESS_CLASS_NAME:-traefik}"
INGRESS_NAMESPACE="${INGRESS_NAMESPACE:-kube-system}"
INGRESS_SERVICE_NAME="${INGRESS_SERVICE_NAME:-traefik}"
INGRESS_LOCAL_PORT="${INGRESS_LOCAL_PORT:-18080}"
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
	echo "e2e-k3d-fusekiui-ingress failed; collecting diagnostics" >&2

	if [[ -s "${MANAGER_LOG}" ]]; then
		print_section "manager log" cat "${MANAGER_LOG}"
	fi

	if ! cluster_context_exists; then
		echo "kubectl context ${KUBECTL_CONTEXT} is unavailable; skipping cluster diagnostics" >&2
		return 0
	fi

	print_section "ingress controller services" kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" get svc
	print_section "ingress controller pods" kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" get pods
	print_section "ingress controller logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" logs deployment/"${INGRESS_SERVICE_NAME}"

	if ! namespace_exists; then
		echo "namespace ${NAMESPACE} is unavailable; skipping namespace diagnostics" >&2
		return 0
	fi

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiuis,fusekiservers,datasets,deploy,svc,ingress,jobs,pods
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "fusekiui yaml" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o yaml
	print_section "ingress yaml" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get ingress standalone-ui -o yaml
	print_section "service yaml" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get service standalone-ui -o yaml
	print_section "fuseki describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe deployment standalone
	print_section "fuseki logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/standalone
	if [[ -s "${UI_RESPONSE_FILE}" ]]; then
		print_section "last ingress ui response" cat "${UI_RESPONSE_FILE}"
	fi
	if [[ -s "${INGRESS_FORWARD_LOG}" ]]; then
		print_section "ingress port-forward log" cat "${INGRESS_FORWARD_LOG}"
	fi
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	for pid_var in INGRESS_FORWARD_PID MANAGER_PID; do
		if [[ -n "${!pid_var:-}" ]]; then
			kill "${!pid_var}" >/dev/null 2>&1 || true
			wait "${!pid_var}" >/dev/null 2>&1 || true
		fi
	done
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

wait_for_ingress_controller() {
	for _ in $(seq 1 90); do
		if kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" get service "${INGRESS_SERVICE_NAME}" >/dev/null 2>&1 && \
			kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" get deployment "${INGRESS_SERVICE_NAME}" >/dev/null 2>&1; then
			if kubectl --context "${KUBECTL_CONTEXT}" -n "${INGRESS_NAMESPACE}" rollout status deployment/"${INGRESS_SERVICE_NAME}" --timeout=10s >/dev/null 2>&1; then
				return 0
			fi
		fi
		sleep 2
	done
	echo "ingress controller ${INGRESS_NAMESPACE}/${INGRESS_SERVICE_NAME} did not become ready" >&2
	return 1
}

wait_for_fusekiui_ready() {
	for _ in $(seq 1 90); do
		phase="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.phase}' 2>/dev/null || true)"
		ingress_reason="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.conditions[?(@.type=="IngressReady")].reason}' 2>/dev/null || true)"
		gateway_reason="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.conditions[?(@.type=="GatewayReady")].reason}' 2>/dev/null || true)"
		service_name="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.serviceName}' 2>/dev/null || true)"
		if [[ "${phase}" == "Ready" && "${ingress_reason}" == "IngressReady" && "${gateway_reason}" == "GatewayNotConfigured" && "${service_name}" == "standalone-ui" ]]; then
			return 0
		fi
		sleep 2
	done
	return 1
}

wait_for_ingress_ui() {
	for _ in $(seq 1 90); do
		if curl --silent --show-error --fail -H "Host: ${INGRESS_HOST}" "http://127.0.0.1:${INGRESS_LOCAL_PORT}/" >"${UI_RESPONSE_FILE}" 2>/dev/null; then
			if grep -q 'Apache Jena Fuseki UI' "${UI_RESPONSE_FILE}" && grep -q '<div id="app"></div>' "${UI_RESPONSE_FILE}"; then
				return 0
			fi
		fi
		sleep 2
	done
	return 1
}

wait_for_ingress_admin_api() {
	for _ in $(seq 1 90); do
		status="$(curl --silent --output /dev/null --write-out '%{http_code}' -u "admin:${ADMIN_PASSWORD}" -H "Host: ${INGRESS_HOST}" "http://127.0.0.1:${INGRESS_LOCAL_PORT}/$/datasets" 2>/dev/null || true)"
		if [[ "${status}" == "200" ]]; then
			return 0
		fi
		sleep 2
	done
	return 1
}

require_tool docker
require_tool k3d
require_tool kubectl
require_tool curl

cd "${ROOT_DIR}"

make generate manifests
make docker-build-fuseki FUSEKI_IMAGE="${FUSEKI_IMAGE}"

k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
k3d image import -c "${CLUSTER_NAME}" "${FUSEKI_IMAGE}"

kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null
kubectl apply -f config/crd/bases
wait_for_ingress_controller

go run ./cmd/manager --metrics-bind-address="${METRICS_BIND_ADDRESS}" --health-probe-bind-address="${PROBE_BIND_ADDRESS}" >"${MANAGER_LOG}" 2>&1 &
MANAGER_PID=$!
wait_for_manager

kubectl create namespace "${NAMESPACE}" >/dev/null 2>&1 || true

{
	printf '%s\n' \
		'apiVersion: v1' \
		'kind: Secret' \
		'metadata:' \
		'  name: admin-secret' \
		"  namespace: ${NAMESPACE}" \
		'type: Opaque' \
		'stringData:' \
		'  username: admin' \
		"  password: ${ADMIN_PASSWORD}" \
		'---' \
		'apiVersion: fuseki.apache.org/v1alpha1' \
		'kind: SecurityProfile' \
		'metadata:' \
		'  name: admin-auth' \
		"  namespace: ${NAMESPACE}" \
		'spec:' \
		'  adminCredentialsSecretRef:' \
		'    name: admin-secret' \
		'---' \
		'apiVersion: fuseki.apache.org/v1alpha1' \
		'kind: Dataset' \
		'metadata:' \
		'  name: primary' \
		"  namespace: ${NAMESPACE}" \
		'spec:' \
		'  name: primary' \
		'  type: TDB2' \
		'---' \
		'apiVersion: fuseki.apache.org/v1alpha1' \
		'kind: FusekiServer' \
		'metadata:' \
		'  name: standalone' \
		"  namespace: ${NAMESPACE}" \
		'spec:' \
		"  image: ${FUSEKI_IMAGE}" \
		'  datasetRefs:' \
		'    - name: primary' \
		'  securityProfileRef:' \
		'    name: admin-auth' \
		'  storage:' \
		'    accessMode: ReadWriteOnce' \
		'    size: 1Gi' \
		'---' \
		'apiVersion: fuseki.apache.org/v1alpha1' \
		'kind: FusekiUI' \
		'metadata:' \
		'  name: standalone-ui' \
		"  namespace: ${NAMESPACE}" \
		'spec:' \
		'  targetRef:' \
		'    kind: FusekiServer' \
		'    name: standalone' \
		'  ingress:' \
		"    host: ${INGRESS_HOST}" \
		"    className: ${INGRESS_CLASS_NAME}" \
		'    annotations:' \
		'      traefik.ingress.kubernetes.io/router.entrypoints: web'
} >"${TMP_DIR}/scenario.yaml"

kubectl apply -f "${TMP_DIR}/scenario.yaml"

kubectl rollout status deployment/standalone -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=complete job/server-standalone-primary-bootstrap -n "${NAMESPACE}" --timeout=180s
wait_for_fusekiui_ready

ui_service_name="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.serviceName}')"
ui_ingress_name="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get fusekiui standalone-ui -o jsonpath='{.status.ingressName}')"
if [[ "${ui_service_name}" != "standalone-ui" ]]; then
	echo "expected FusekiUI serviceName standalone-ui, got ${ui_service_name}" >&2
	exit 1
fi
if [[ "${ui_ingress_name}" != "standalone-ui" ]]; then
	echo "expected FusekiUI ingressName standalone-ui, got ${ui_ingress_name}" >&2
	exit 1
fi

kubectl port-forward -n "${INGRESS_NAMESPACE}" service/"${INGRESS_SERVICE_NAME}" "${INGRESS_LOCAL_PORT}":80 >"${INGRESS_FORWARD_LOG}" 2>&1 &
INGRESS_FORWARD_PID=$!
wait_for_ingress_ui
wait_for_ingress_admin_api

printf 'FusekiUI ingress e2e passed\n'
printf 'Ingress host: %s\n' "${INGRESS_HOST}"
printf 'UI service: %s\n' "${ui_service_name}"
printf 'UI ingress: %s\n' "${ui_ingress_name}"