#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-m4-oidc-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-oidc-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
FUSEKI_IMAGE="${FUSEKI_IMAGE:-ghcr.io/larsw/k8s-fuseki-operator/fuseki:e2e}"
DEX_IMAGE="${DEX_IMAGE:-ghcr.io/dexidp/dex:v2.41.1}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-e2e-admin-password}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TMP_DIR="$(mktemp -d)"
MANAGER_LOG="${TMP_DIR}/manager.log"
DEX_FORWARD_LOG="${TMP_DIR}/dex-port-forward.log"
FUSEKI_FORWARD_LOG="${TMP_DIR}/fuseki-port-forward.log"
METRICS_BIND_ADDRESS="${METRICS_BIND_ADDRESS:-127.0.0.1:0}"
PROBE_BIND_ADDRESS="${PROBE_BIND_ADDRESS:-127.0.0.1:0}"
DEX_ISSUER="http://dex.${NAMESPACE}.svc.cluster.local:5556/dex"

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
	echo "e2e-k3d-m4-oidc failed; collecting diagnostics" >&2

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

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pods,deploy,svc,jobs,cm
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "dex describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe deployment dex
	print_section "dex logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/dex
	print_section "fuseki describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe deployment standalone
	print_section "fuseki logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/standalone
	print_section "security profile config" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get configmap admin-auth-security -o yaml
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	for pid_var in DEX_FORWARD_PID FUSEKI_FORWARD_PID MANAGER_PID; do
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

jsonpath_env_value() {
	local workload=$1
	local name=$2
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${workload}" -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='${name}')].value}"
}

require_tool docker
require_tool k3d
require_tool kubectl
require_tool curl

cd "${ROOT_DIR}"

make generate manifests
make docker-build-fuseki FUSEKI_IMAGE="${FUSEKI_IMAGE}"
docker pull "${DEX_IMAGE}" >/dev/null

k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
k3d image import -c "${CLUSTER_NAME}" "${FUSEKI_IMAGE}" "${DEX_IMAGE}"

kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null
kubectl apply -f config/crd/bases

go run ./cmd/manager --metrics-bind-address="${METRICS_BIND_ADDRESS}" --health-probe-bind-address="${PROBE_BIND_ADDRESS}" >"${MANAGER_LOG}" 2>&1 &
MANAGER_PID=$!
wait_for_manager

cat >"${TMP_DIR}/namespace.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
EOF

kubectl apply -f "${TMP_DIR}/namespace.yaml"

cat >"${TMP_DIR}/dex-config.yaml" <<EOF
issuer: ${DEX_ISSUER}
storage:
  type: memory
web:
  http: 0.0.0.0:5556
oauth2:
  passwordConnector: local
enablePasswordDB: true
staticPasswords:
  - email: admin@example.com
    hash: "\$2a\$10\$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
    username: admin
    userID: 08a8684b-db88-4b73-90a9-3cd1661f5466
staticClients:
  - id: fuseki-e2e
    secret: fuseki-e2e-secret
    name: Fuseki E2E
    redirectURIs:
      - http://127.0.0.1:18080/callback
EOF

kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" create configmap dex-config --from-file=config.yaml="${TMP_DIR}/dex-config.yaml" --dry-run=client -o yaml | kubectl apply -f -

cat >"${TMP_DIR}/scenario.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dex
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dex
  template:
    metadata:
      labels:
        app: dex
    spec:
      containers:
        - name: dex
          image: ${DEX_IMAGE}
          args:
            - dex
            - serve
            - /etc/dex/config.yaml
          ports:
            - name: http
              containerPort: 5556
          volumeMounts:
            - name: config
              mountPath: /etc/dex
      volumes:
        - name: config
          configMap:
            name: dex-config
---
apiVersion: v1
kind: Service
metadata:
  name: dex
  namespace: ${NAMESPACE}
spec:
  selector:
    app: dex
  ports:
    - name: http
      port: 5556
      targetPort: http
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
  oidcIssuerURL: ${DEX_ISSUER}
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

kubectl rollout status deployment/dex -n "${NAMESPACE}" --timeout=180s
kubectl port-forward -n "${NAMESPACE}" service/dex 15556:5556 >"${DEX_FORWARD_LOG}" 2>&1 &
DEX_FORWARD_PID=$!
wait_for_http 'http://127.0.0.1:15556/dex/.well-known/openid-configuration'

kubectl rollout status deployment/standalone -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=complete job/server-standalone-example-dataset-bootstrap -n "${NAMESPACE}" --timeout=180s

well_known_json="$(curl --silent --show-error --fail 'http://127.0.0.1:15556/dex/.well-known/openid-configuration')"
if ! grep -q "${DEX_ISSUER}" <<<"${well_known_json}"; then
	echo "expected Dex discovery document to advertise issuer ${DEX_ISSUER}" >&2
	echo "${well_known_json}" >&2
	exit 1
fi

token_json="$(curl --silent --show-error --fail \
	--user 'fuseki-e2e:fuseki-e2e-secret' \
	-H 'Content-Type: application/x-www-form-urlencoded' \
	--data-urlencode 'grant_type=password' \
	--data-urlencode 'scope=openid profile email' \
	--data-urlencode 'username=admin@example.com' \
	--data-urlencode 'password=password' \
	'http://127.0.0.1:15556/dex/token')"
if ! grep -q '"id_token"' <<<"${token_json}"; then
	echo "expected Dex password grant to return an ID token" >&2
	echo "${token_json}" >&2
	exit 1
fi

security_properties="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get configmap admin-auth-security -o go-template='{{index .data "security.properties"}}')"
if ! grep -q "oidc.issuerURL=${DEX_ISSUER}" <<<"${security_properties}"; then
	echo "expected SecurityProfile configmap to contain Dex issuer" >&2
	echo "${security_properties}" >&2
	exit 1
fi

runtime_issuer="$(jsonpath_env_value deployment/standalone SECURITY_PROFILE_OIDC_ISSUER)"
if [[ "${runtime_issuer}" != "${DEX_ISSUER}" ]]; then
	echo "expected FusekiServer runtime OIDC issuer ${DEX_ISSUER}, got ${runtime_issuer}" >&2
	exit 1
fi

bootstrap_issuer="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get job server-standalone-example-dataset-bootstrap -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='SECURITY_PROFILE_OIDC_ISSUER')].value}")"
if [[ "${bootstrap_issuer}" != "${DEX_ISSUER}" ]]; then
	echo "expected bootstrap job OIDC issuer ${DEX_ISSUER}, got ${bootstrap_issuer}" >&2
	exit 1
fi

kubectl port-forward -n "${NAMESPACE}" service/standalone 13030:3030 >"${FUSEKI_FORWARD_LOG}" 2>&1 &
FUSEKI_FORWARD_PID=$!
wait_for_http 'http://127.0.0.1:13030/$/ping'

curl --silent --show-error --fail \
	-u "admin:${ADMIN_PASSWORD}" \
	-H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
	--data 'dbName=probe&dbType=tdb2' \
	'http://127.0.0.1:13030/$/datasets' >/dev/null

printf 'M4 OIDC projection e2e passed\n'
printf 'Dex issuer: %s\n' "${DEX_ISSUER}"
printf 'Runtime issuer: %s\n' "${runtime_issuer}"
