#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-m3-recovery-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
FUSEKI_IMAGE="${FUSEKI_IMAGE:-ghcr.io/larsw/k8s-fuseki-operator/fuseki:e2e}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-e2e-admin-password}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
METRICS_BIND_ADDRESS="${METRICS_BIND_ADDRESS:-127.0.0.1:0}"
PROBE_BIND_ADDRESS="${PROBE_BIND_ADDRESS:-127.0.0.1:0}"
FUSEKI_PORT_FORWARD_PORT="${FUSEKI_PORT_FORWARD_PORT:-}"
TMP_DIR="$(mktemp -d)"
PORT_FORWARD_LOG="${TMP_DIR}/port-forward.log"
MANAGER_START_COUNT=0

print_section() {
	local title=$1
	shift
	echo >&2
	echo "===== ${title} =====" >&2
	"$@" >&2 || true
}

log_step() {
	echo >&2
	echo "--> $*" >&2
}

cluster_context_exists() {
	kubectl config get-contexts "${KUBECTL_CONTEXT}" >/dev/null 2>&1
}

namespace_exists() {
	kubectl --context "${KUBECTL_CONTEXT}" get namespace "${NAMESPACE}" >/dev/null 2>&1
}

manager_logs() {
	compgen -G "${TMP_DIR}/manager-*.log" || true
}

dump_failure_diagnostics() {
	[[ "${DIAGNOSTICS_EMITTED:-0}" == "1" ]] && return 0
	DIAGNOSTICS_EMITTED=1

	echo >&2
	echo "e2e-k3d-m3-recovery failed; collecting diagnostics" >&2

	for log_file in $(manager_logs); do
		if [[ -s "${log_file}" ]]; then
			print_section "$(basename "${log_file}")" cat "${log_file}"
		fi
	done

	if ! cluster_context_exists; then
		echo "kubectl context ${KUBECTL_CONTEXT} is unavailable; skipping cluster diagnostics" >&2
		return 0
	fi

	if ! namespace_exists; then
		echo "namespace ${NAMESPACE} is unavailable; skipping namespace diagnostics" >&2
		return 0
	fi

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pods,deployments,services,jobs,cronjobs,configmaps
	print_section "custom resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get ingestpipelines.fuseki.apache.org,shaclpolicies.fuseki.apache.org,fusekiservers.fuseki.apache.org,datasets.fuseki.apache.org,securityprofiles.fuseki.apache.org
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "ingest summary" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get configmap retry-ingest-ingest-summary -o yaml
	print_section "failed ingest job" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe job retry-ingest-ingest
	print_section "scheduled ingest jobs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get jobs -o wide
	print_section "server deployment" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe deployment example-server
	print_section "source pod" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe pod source
	print_section "source pod logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs pod/source
	print_section "fuseki logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/example-server
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	if [[ -n "${PORT_FORWARD_PID:-}" ]]; then
		kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
		wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
	fi
	stop_manager || true
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
		if curl --silent --fail "${url}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done
	return 1
}

select_local_port() {
	local port
	if [[ -n "${FUSEKI_PORT_FORWARD_PORT}" ]]; then
		echo "${FUSEKI_PORT_FORWARD_PORT}"
		return 0
	fi

	for port in $(seq 13030 13130); do
		if ! (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
			echo "${port}"
			return 0
		fi
	done

	echo "unable to find a free local port for the Fuseki service port-forward" >&2
	return 1
}

start_manager() {
	MANAGER_START_COUNT=$((MANAGER_START_COUNT + 1))
	CURRENT_MANAGER_LOG="${TMP_DIR}/manager-${MANAGER_START_COUNT}.log"
	log_step "Starting local manager instance ${MANAGER_START_COUNT}"
	go run ./cmd/manager --metrics-bind-address="${METRICS_BIND_ADDRESS}" --health-probe-bind-address="${PROBE_BIND_ADDRESS}" >"${CURRENT_MANAGER_LOG}" 2>&1 &
	MANAGER_PID=$!
	wait_for_manager "${CURRENT_MANAGER_LOG}"
}

stop_manager() {
	if [[ -n "${MANAGER_PID:-}" ]]; then
		kill "${MANAGER_PID}" >/dev/null 2>&1 || true
		wait "${MANAGER_PID}" >/dev/null 2>&1 || true
		unset MANAGER_PID
	fi
}

wait_for_manager() {
	local manager_log=$1
	for _ in $(seq 1 30); do
		if grep -q 'starting manager' "${manager_log}" 2>/dev/null; then
			return 0
		fi
		sleep 1
	done
	echo "manager did not start successfully" >&2
		cat "${manager_log}" >&2 || true
		return 1
}

wait_for_jsonpath_value() {
	local resource=$1
	local name=$2
	local jsonpath=$3
	local expected=$4
	local attempts=${5:-90}
	local current

	for _ in $(seq 1 "${attempts}"); do
		current="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${resource}" "${name}" -o "jsonpath=${jsonpath}" 2>/dev/null || true)"
		if [[ "${current}" == "${expected}" ]]; then
			return 0
		fi
		sleep 2
	done

	echo "timed out waiting for ${resource}/${name} ${jsonpath}=${expected}" >&2
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${resource}" "${name}" -o yaml >&2 || true
	return 1
}

wait_for_absence() {
	local resource=$1
	local name=$2
	local attempts=${3:-60}

	for _ in $(seq 1 "${attempts}"); do
		if ! kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${resource}" "${name}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done

	echo "timed out waiting for ${resource}/${name} deletion" >&2
	return 1
}

wait_for_resource() {
	local resource=$1
	local name=$2
	local attempts=${3:-60}

	for _ in $(seq 1 "${attempts}"); do
		if kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get "${resource}" "${name}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done

	echo "timed out waiting for ${resource}/${name}" >&2
	return 1
}

latest_scheduled_job_name() {
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get jobs -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
		| grep '^retry-ingest-ingest-' \
		| sort \
		| tail -n 1
}

require_tool docker
require_tool k3d
require_tool kubectl
require_tool curl

cd "${ROOT_DIR}"

log_step "Building local Fuseki image"
make docker-build-fuseki FUSEKI_IMAGE="${FUSEKI_IMAGE}"

log_step "Creating k3d cluster ${CLUSTER_NAME}"
k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
k3d image import -c "${CLUSTER_NAME}" "${FUSEKI_IMAGE}"

kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null
log_step "Applying checked-in CRDs"
for crd in config/crd/bases/fuseki.apache.org_*.yaml; do
	kubectl apply -f "${crd}"
done

start_manager

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
apiVersion: v1
kind: ConfigMap
metadata:
  name: source-data
  namespace: ${NAMESPACE}
data:
  events.ttl: |
    @prefix ex: <https://example.com/ns#> .

    <urn:example:event/1> a ex:Event ;
      ex:id "1" ;
      ex:name "scheduled-retry" .
---
apiVersion: v1
kind: Pod
metadata:
  name: source
  namespace: ${NAMESPACE}
  labels:
    app: source
spec:
  containers:
    - name: source
      image: busybox:1.36
      command:
        - sh
        - -ceu
        - exec httpd -f -p 8080 -h /data
      ports:
        - containerPort: 8080
      readinessProbe:
        httpGet:
          path: /events.ttl
          port: 8080
      volumeMounts:
        - name: source-data
          mountPath: /data
  volumes:
    - name: source-data
      configMap:
        name: source-data
---
apiVersion: v1
kind: Service
metadata:
  name: source
  namespace: ${NAMESPACE}
spec:
  selector:
    app: source
  ports:
    - name: http
      port: 8080
      targetPort: 8080
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
  name: example-server
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
---
apiVersion: fuseki.apache.org/v1alpha1
kind: SHACLPolicy
metadata:
  name: example-shaclpolicy
  namespace: ${NAMESPACE}
spec:
  sources:
    - type: Inline
      inline: |
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <https://example.com/ns#> .

        ex:EventShape
          a sh:NodeShape ;
          sh:targetClass ex:Event ;
          sh:property [
            sh:path ex:id ;
            sh:minCount 1 ;
          ] .
  failureAction: Reject
EOF

cat >"${TMP_DIR}/failed-pipeline.yaml" <<EOF
apiVersion: fuseki.apache.org/v1alpha1
kind: IngestPipeline
metadata:
  name: retry-ingest
  namespace: ${NAMESPACE}
spec:
  target:
    datasetRef:
      name: example-dataset
  source:
    type: URL
    uri: http://source.${NAMESPACE}.svc.cluster.local:8080/missing.ttl
    format: text/turtle
  shaclPolicyRef:
    name: example-shaclpolicy
EOF

cat >"${TMP_DIR}/scheduled-pipeline.yaml" <<EOF
apiVersion: fuseki.apache.org/v1alpha1
kind: IngestPipeline
metadata:
  name: retry-ingest
  namespace: ${NAMESPACE}
spec:
  target:
    datasetRef:
      name: example-dataset
  source:
    type: URL
    uri: http://source.${NAMESPACE}.svc.cluster.local:8080/events.ttl
    format: text/turtle
  shaclPolicyRef:
    name: example-shaclpolicy
  schedule: "* * * * *"
EOF

log_step "Applying recovery scenario resources"
kubectl apply -f "${TMP_DIR}/scenario.yaml"

log_step "Waiting for source pod"
kubectl wait --for=condition=Ready pod/source -n "${NAMESPACE}" --timeout=180s
log_step "Waiting for SHACLPolicy Configured status"
kubectl wait --for=condition=Configured shaclpolicies.fuseki.apache.org/example-shaclpolicy -n "${NAMESPACE}" --timeout=180s
log_step "Waiting for FusekiServer rollout"
kubectl rollout status deployment/example-server -n "${NAMESPACE}" --timeout=240s
log_step "Waiting for FusekiServer dataset bootstrap job"
kubectl wait --for=condition=complete job/server-example-server-example-dataset-bootstrap -n "${NAMESPACE}" --timeout=180s

log_step "Creating a failing one-shot ingest pipeline"
kubectl apply -f "${TMP_DIR}/failed-pipeline.yaml"

log_step "Waiting for failed ingest job and failed pipeline phase"
wait_for_jsonpath_value jobs retry-ingest-ingest '{.status.failed}' '1' 90
wait_for_jsonpath_value ingestpipelines.fuseki.apache.org retry-ingest '{.status.phase}' Failed 90

stop_manager
log_step "Updating the ingest pipeline into scheduled mode while the manager is down"
kubectl apply -f "${TMP_DIR}/scheduled-pipeline.yaml"

start_manager

log_step "Waiting for CronJob recovery and one-shot job cleanup"
wait_for_resource cronjobs retry-ingest-ingest 90
wait_for_absence jobs retry-ingest-ingest 90
wait_for_jsonpath_value ingestpipelines.fuseki.apache.org retry-ingest '{.status.phase}' Running 90
wait_for_jsonpath_value configmaps retry-ingest-ingest-summary '{.data.targetKind}' CronJob 90
wait_for_jsonpath_value configmaps retry-ingest-ingest-summary '{.data.schedule}' '* * * * *' 90

log_step "Waiting for the first scheduled ingest job"
scheduled_job_name=""
for _ in $(seq 1 90); do
	scheduled_job_name="$(latest_scheduled_job_name || true)"
	if [[ -n "${scheduled_job_name}" ]]; then
		break
	fi
	scheduled_job_name=""
	sleep 2
done

if [[ -z "${scheduled_job_name}" ]]; then
	echo "timed out waiting for a scheduled ingest job" >&2
	exit 1
fi

log_step "Waiting for scheduled ingest job ${scheduled_job_name}"
kubectl wait --for=condition=complete "job/${scheduled_job_name}" -n "${NAMESPACE}" --timeout=240s

fuseki_local_port="$(select_local_port)"
log_step "Port-forwarding Fuseki and querying imported data"
kubectl port-forward -n "${NAMESPACE}" svc/example-server "${fuseki_local_port}:3030" >"${PORT_FORWARD_LOG}" 2>&1 &
PORT_FORWARD_PID=$!
wait_for_http "http://127.0.0.1:${fuseki_local_port}/$/ping"

query_result="$(curl --silent --show-error --fail \
	-u "admin:${ADMIN_PASSWORD}" \
	--get \
	--data-urlencode 'query=SELECT ?name WHERE { <urn:example:event/1> <https://example.com/ns#name> ?name }' \
	-H 'Accept: text/csv' \
	"http://127.0.0.1:${fuseki_local_port}/primary/query")"

if ! grep -q 'scheduled-retry' <<<"${query_result}"; then
	echo "expected scheduled ingest data in Fuseki query response" >&2
	echo "${query_result}" >&2
	exit 1
fi

echo "M3 recovery k3d e2e passed"
echo "Scheduled ingest job: ${scheduled_job_name}"
echo "Fuseki query result: ${query_result}"