#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-fuseki-m5-backup-restore-e2e}"
NAMESPACE="${NAMESPACE:-fuseki-backup-e2e}"
KUBECTL_CONTEXT="k3d-${CLUSTER_NAME}"
RDF_DELTA_IMAGE="${RDF_DELTA_IMAGE:-ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:e2e}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2025-02-07T23-21-09Z}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:RELEASE.2025-07-21T05-28-08Z}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
TMP_DIR="$(mktemp -d)"
MANAGER_LOG="${TMP_DIR}/manager.log"
METRICS_BIND_ADDRESS="${METRICS_BIND_ADDRESS:-127.0.0.1:0}"
PROBE_BIND_ADDRESS="${PROBE_BIND_ADDRESS:-127.0.0.1:0}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
MINIO_BUCKET="${MINIO_BUCKET:-fuseki-backups}"
DELTA_PORT_FORWARD_LOG="${TMP_DIR}/delta-port-forward.log"

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
	echo "e2e-k3d-m5-backup-restore failed; collecting diagnostics" >&2

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

	print_section "namespace resources" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get backuppolicies,restorerequests,rdfdeltaservers,cronjobs,jobs,pods,sts,svc,secrets
	print_section "namespace events" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp
	print_section "rdf delta describe" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" describe statefulset example-delta
	print_section "restore request" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get restorerequest restore-snapshot -o yaml
	print_section "backup policy" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get backuppolicy example-backup -o yaml
	print_section "backup cronjob" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get cronjob example-delta-backup -o yaml
	print_section "minio logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs deployment/minio
	if kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get job example-delta-backup-manual >/dev/null 2>&1; then
		print_section "manual backup job logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs job/example-delta-backup-manual
	fi
	if kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get job restore-snapshot-restore >/dev/null 2>&1; then
		print_section "restore job logs" kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs job/restore-snapshot-restore
	fi
}

cleanup() {
	local exit_code=$?
	if [[ ${exit_code} -ne 0 ]]; then
		dump_failure_diagnostics
	fi
	if [[ -n "${MANAGER_PID:-}" ]]; then
		kill "${MANAGER_PID}" >/dev/null 2>&1 || true
		wait "${MANAGER_PID}" >/dev/null 2>&1 || true
	fi
	if [[ -n "${DELTA_PORT_FORWARD_PID:-}" ]]; then
		kill "${DELTA_PORT_FORWARD_PID}" >/dev/null 2>&1 || true
		wait "${DELTA_PORT_FORWARD_PID}" >/dev/null 2>&1 || true
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

wait_for_jsonpath() {
	local resource=$1
	local jsonpath=$2
	local expected=$3
	for _ in $(seq 1 90); do
		value="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get ${resource} -o jsonpath="${jsonpath}" 2>/dev/null || true)"
		if [[ "${value}" == "${expected}" ]]; then
			return 0
		fi
		sleep 2
	done
	return 1
}

wait_for_nonempty_jsonpath() {
	local resource=$1
	local jsonpath=$2
	for _ in $(seq 1 90); do
		value="$(kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get ${resource} -o jsonpath="${jsonpath}" 2>/dev/null || true)"
		if [[ -n "${value}" ]]; then
			printf '%s' "${value}"
			return 0
		fi
		sleep 2
	done
	return 1
}

run_minio_mc() {
	local pod_name=$1
	local command=$2
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" delete pod "${pod_name}" --ignore-not-found >/dev/null 2>&1 || true
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" run "${pod_name}" --restart=Never --image="${MINIO_MC_IMAGE}" --env "MC_HOST_backup=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio.${NAMESPACE}.svc.cluster.local:9000" --command -- sh -ec "${command}" >/dev/null
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"${pod_name}" --timeout=90s >/dev/null
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" logs pod/"${pod_name}"
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" delete pod "${pod_name}" --ignore-not-found >/dev/null 2>&1 || true
}

wait_for_minio_bucket() {
	for _ in $(seq 1 60); do
		output="$(run_minio_mc minio-ls 'mc ls backup/'"${MINIO_BUCKET}"'/rdf-delta/'"${NAMESPACE}"'/example-delta/' 2>/dev/null || true)"
		if [[ -n "${output}" ]]; then
			printf '%s' "${output}"
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

import_k3d_image_if_present() {
	local image=$1
	if docker image inspect "${image}" >/dev/null 2>&1; then
		k3d image import -c "${CLUSTER_NAME}" "${image}" >/dev/null
	fi
}

wait_for_http() {
	local url=$1
	for _ in $(seq 1 60); do
		if curl --silent --show-error --fail "${url}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done
	return 1
}

start_delta_port_forward() {
	kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" port-forward service/example-delta 13066:1066 >"${DELTA_PORT_FORWARD_LOG}" 2>&1 &
	DELTA_PORT_FORWARD_PID=$!
	wait_for_http 'http://127.0.0.1:13066/$/ping'
}

stop_delta_port_forward() {
	if [[ -n "${DELTA_PORT_FORWARD_PID:-}" ]]; then
		kill "${DELTA_PORT_FORWARD_PID}" >/dev/null 2>&1 || true
		wait "${DELTA_PORT_FORWARD_PID}" >/dev/null 2>&1 || true
		unset DELTA_PORT_FORWARD_PID
	fi
}

delta_update() {
	local update=$1
	curl --silent --show-error --fail -X POST -H 'Content-Type: application/sparql-update' --data "${update}" 'http://127.0.0.1:13066/delta/update' >/dev/null
}

delta_query_csv_last_value() {
	local query=$1
	local value
	value="$(curl --silent --show-error --fail --get --data-urlencode "query=${query}" -H 'Accept: text/csv' 'http://127.0.0.1:13066/delta/query' | tail -n +2 | tail -n 1 | tr -d '\r')"
	value="${value#\"}"
	value="${value%\"}"
	printf '%s' "${value}"
}

assert_delta_literal() {
	local expected=$1
	local actual count
	actual="$(delta_query_csv_last_value 'SELECT ?o WHERE { <urn:backup:s> <urn:backup:p> ?o } ORDER BY ?o')"
	count="$(delta_query_csv_last_value 'SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }')"
	if [[ "${actual}" != "${expected}" ]]; then
		echo "expected RDF Delta literal ${expected}, got ${actual}" >&2
		exit 1
	fi
	if [[ "${count}" != "1" ]]; then
		echo "expected RDF Delta triple count 1, got ${count}" >&2
		exit 1
	fi
}

cd "${ROOT_DIR}"

make generate manifests
make docker-build-rdf-delta RDF_DELTA_IMAGE="${RDF_DELTA_IMAGE}"

k3d cluster delete "${CLUSTER_NAME}" >/dev/null 2>&1 || true
k3d cluster create "${CLUSTER_NAME}" --wait
import_k3d_image_if_present "${RDF_DELTA_IMAGE}"
import_k3d_image_if_present "${MINIO_IMAGE}"
import_k3d_image_if_present "${MINIO_MC_IMAGE}"

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
  name: backup-creds
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  accessKey: ${MINIO_ROOT_USER}
  secretKey: ${MINIO_ROOT_PASSWORD}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: ${MINIO_IMAGE}
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              value: ${MINIO_ROOT_USER}
            - name: MINIO_ROOT_PASSWORD
              value: ${MINIO_ROOT_PASSWORD}
          ports:
            - containerPort: 9000
              name: api
            - containerPort: 9001
              name: console
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${NAMESPACE}
spec:
  selector:
    app: minio
  ports:
    - name: api
      port: 9000
      targetPort: api
---
apiVersion: fuseki.apache.org/v1alpha1
kind: BackupPolicy
metadata:
  name: example-backup
  namespace: ${NAMESPACE}
spec:
  schedule: "0 2 * * *"
  s3:
    endpoint: http://minio.${NAMESPACE}.svc.cluster.local:9000
    bucket: ${MINIO_BUCKET}
    prefix: rdf-delta
    credentialsSecretRef:
      name: backup-creds
    insecure: true
  retention:
    maxBackups: 5
---
apiVersion: fuseki.apache.org/v1alpha1
kind: RDFDeltaServer
metadata:
  name: example-delta
  namespace: ${NAMESPACE}
spec:
  image: ${RDF_DELTA_IMAGE}
  replicas: 1
  servicePort: 1066
  backupPolicyRef:
    name: example-backup
  storage:
    accessMode: ReadWriteOnce
    size: 1Gi
EOF

kubectl apply -f "${TMP_DIR}/scenario.yaml"
kubectl rollout status deployment/minio -n "${NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/example-delta -n "${NAMESPACE}" --timeout=180s
start_delta_port_forward

run_minio_mc minio-mkbucket 'mc mb --ignore-existing backup/'"${MINIO_BUCKET}"'' >/dev/null

wait_for_jsonpath backuppolicy/example-backup '{.status.phase}' Ready
wait_for_jsonpath rdfdeltaserver/example-delta '{.status.conditions[?(@.type=="BackupReady")].reason}' BackupCronJobReady

delta_update 'INSERT DATA { <urn:backup:s> <urn:backup:p> "original" }'
assert_delta_literal original

kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" create job --from=cronjob/example-delta-backup example-delta-backup-manual
kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" wait --for=condition=complete job/example-delta-backup-manual --timeout=180s

object_listing="$(wait_for_minio_bucket)"
backup_object="$(printf '%s\n' "${object_listing}" | awk '{print $NF}' | tail -n1)"
backup_object="${backup_object%/}"
if [[ -z "${backup_object}" ]]; then
	echo "expected backup object to exist in MinIO" >&2
	exit 1
fi

delta_update 'DELETE { <urn:backup:s> <urn:backup:p> ?o } INSERT { <urn:backup:s> <urn:backup:p> "mutated" } WHERE { <urn:backup:s> <urn:backup:p> ?o }'
assert_delta_literal mutated
stop_delta_port_forward

cat >"${TMP_DIR}/restore.yaml" <<EOF
apiVersion: fuseki.apache.org/v1alpha1
kind: RestoreRequest
metadata:
  name: restore-snapshot
  namespace: ${NAMESPACE}
spec:
  targetRef:
    kind: RDFDeltaServer
    name: example-delta
  backupObject: ${backup_object}
EOF

kubectl apply -f "${TMP_DIR}/restore.yaml"

wait_for_jsonpath rdfdeltaserver/example-delta '{.status.phase}' Restoring
wait_for_jsonpath restorerequest/restore-snapshot '{.status.phase}' Succeeded
kubectl --context "${KUBECTL_CONTEXT}" -n "${NAMESPACE}" rollout status statefulset/example-delta --timeout=180s
start_delta_port_forward

assert_delta_literal original

printf 'M5 backup/restore e2e passed\n'
printf 'Backup object: %s\n' "${backup_object}"