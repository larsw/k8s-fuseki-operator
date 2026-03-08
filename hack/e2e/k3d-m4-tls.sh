#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-m4-tls-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-tls-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
FUSEKI_IMAGE="${FUSEKI_IMAGE:-ghcr.io/example/fuseki-operator/fuseki:e2e}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-e2e-admin-password}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TMP_DIR="$(mktemp -d)"
MANAGER_LOG="${TMP_DIR}/manager.log"
FUSEKI_FORWARD_LOG="${TMP_DIR}/fuseki-port-forward.log"
TLS_CERT="${TMP_DIR}/tls.crt"
TLS_KEY="${TMP_DIR}/tls.key"
TLS_CA="${TMP_DIR}/ca.crt"
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
	echo "e2e-k3d-m4-tls failed; collecting diagnostics" >&2

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

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pods,deploy,svc,jobs,cm,secrets
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "fuseki describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe deployment standalone
	print_section "fuseki logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/standalone
	print_section "security profile config" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get configmap admin-auth-security -o yaml
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	for pid_var in FUSEKI_FORWARD_PID MANAGER_PID; do
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

wait_for_url() {
	local url=$1
	shift
	for _ in $(seq 1 60); do
		if curl --silent --fail "$@" "${url}" >/dev/null 2>&1; then
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

jsonpath_env_value() {
	local workload=$1
	local name=$2
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${workload}" -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='${name}')].value}"
}

generate_tls_assets() {
	cat >"${TMP_DIR}/tls.cnf" <<EOF
[req]
distinguished_name = dn
x509_extensions = v3_req
prompt = no

[dn]
CN = standalone.${NAMESPACE}.svc.cluster.local

[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = standalone
DNS.2 = standalone.${NAMESPACE}
DNS.3 = standalone.${NAMESPACE}.svc
DNS.4 = standalone.${NAMESPACE}.svc.cluster.local
IP.1 = 127.0.0.1
EOF

	openssl req -x509 -nodes -newkey rsa:2048 -days 2 \
		-keyout "${TLS_KEY}" \
		-out "${TLS_CERT}" \
		-config "${TMP_DIR}/tls.cnf" >/dev/null 2>&1
	cp "${TLS_CERT}" "${TLS_CA}"
}

require_tool docker
require_tool k3d
require_tool kubectl
require_tool curl
require_tool openssl

cd "${ROOT_DIR}"

make generate manifests
make docker-build-fuseki FUSEKI_IMAGE="${FUSEKI_IMAGE}"

k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
k3d image import -c "${CLUSTER_NAME}" "${FUSEKI_IMAGE}"

kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null
kubectl apply -f config/crd/bases

go run ./cmd/manager --metrics-bind-address="${METRICS_BIND_ADDRESS}" --health-probe-bind-address="${PROBE_BIND_ADDRESS}" >"${MANAGER_LOG}" 2>&1 &
MANAGER_PID=$!
wait_for_manager

generate_tls_assets

kubectl create namespace "${NAMESPACE}" >/dev/null 2>&1 || true
kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" create secret generic fuseki-tls \
	--type kubernetes.io/tls \
	--from-file=tls.crt="${TLS_CERT}" \
	--from-file=tls.key="${TLS_KEY}" \
	--from-file=ca.crt="${TLS_CA}" \
	--dry-run=client -o yaml | kubectl apply -f -

cat >"${TMP_DIR}/scenario.yaml" <<EOF
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
  tlsSecretRef:
    name: fuseki-tls
---
apiVersion: fuseki.apache.org/v1alpha1
kind: Dataset
metadata:
  name: example-dataset
  namespace: ${NAMESPACE}
spec:
  name: primary
  type: TDB2
---
apiVersion: fuseki.apache.org/v1alpha1
kind: FusekiServer
metadata:
  name: standalone
  namespace: ${NAMESPACE}
spec:
  image: ${FUSEKI_IMAGE}
  datasetRefs:
    - name: example-dataset
  securityProfileRef:
    name: admin-auth
  storage:
    accessMode: ReadWriteOnce
    size: 1Gi
EOF

kubectl apply -f "${TMP_DIR}/scenario.yaml"

kubectl rollout status deployment/standalone -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=complete job/server-standalone-example-dataset-bootstrap -n "${NAMESPACE}" --timeout=180s

security_properties="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get configmap admin-auth-security -o go-template='{{index .data "security.properties"}}')"
if ! grep -q "tls.certFile=/fuseki-extra/security/tls/tls.crt" <<<"${security_properties}"; then
	echo "expected SecurityProfile configmap to contain TLS certificate path" >&2
	echo "${security_properties}" >&2
	exit 1
fi

runtime_scheme="$(jsonpath_env_value deployment/standalone FUSEKI_SERVER_SCHEME)"
if [[ "${runtime_scheme}" != "https" ]]; then
	echo "expected FusekiServer runtime scheme https, got ${runtime_scheme}" >&2
	exit 1
fi

runtime_cert_file="$(jsonpath_env_value deployment/standalone SECURITY_PROFILE_TLS_CERT_FILE)"
if [[ "${runtime_cert_file}" != "/fuseki-extra/security/tls/tls.crt" ]]; then
	echo "expected FusekiServer runtime cert file, got ${runtime_cert_file}" >&2
	exit 1
fi

bootstrap_url="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get job server-standalone-example-dataset-bootstrap -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='FUSEKI_WRITE_URL')].value}")"
if [[ "${bootstrap_url}" != "https://standalone:3030" ]]; then
	echo "expected bootstrap job write URL to use https, got ${bootstrap_url}" >&2
	exit 1
fi

kubectl port-forward -n "${NAMESPACE}" service/standalone 13030:3030 >"${FUSEKI_FORWARD_LOG}" 2>&1 &
FUSEKI_FORWARD_PID=$!
wait_for_url 'https://127.0.0.1:13030/$/ping' --cacert "${TLS_CA}"

if curl --silent --fail 'http://127.0.0.1:13030/$/ping' >/dev/null 2>&1; then
	echo "expected plain HTTP to fail once TLS is enabled" >&2
	exit 1
fi

curl --silent --show-error --fail \
	--cacert "${TLS_CA}" \
	-u "admin:${ADMIN_PASSWORD}" \
	-H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
	--data 'dbName=probe&dbType=tdb2' \
	'https://127.0.0.1:13030/$/datasets' >/dev/null

printf 'M4 TLS listener e2e passed\n'
printf 'Runtime scheme: %s\n' "${runtime_scheme}"
printf 'TLS cert file: %s\n' "${runtime_cert_file}"