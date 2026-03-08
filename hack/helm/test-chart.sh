#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART_DIR="${ROOT_DIR}/charts/fuseki-operator"
TMP_DIR="$(mktemp -d)"
DEFAULT_RENDER="${TMP_DIR}/default.yaml"
OVERRIDE_RENDER="${TMP_DIR}/override.yaml"
OVERRIDE_VALUES="${TMP_DIR}/override-values.yaml"

cleanup() {
	rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

require_tool() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "missing required tool: $1" >&2
		exit 1
	}
}

assert_contains() {
	local file=$1
	local needle=$2
	if ! grep -Fq -- "${needle}" "${file}"; then
		echo "expected rendered chart to contain: ${needle}" >&2
		exit 1
	fi
}

assert_matches() {
	local file=$1
	local pattern=$2
	if ! grep -Eq -- "${pattern}" "${file}"; then
		echo "expected rendered chart to match regex: ${pattern}" >&2
		exit 1
	fi
}

require_tool helm

helm lint "${CHART_DIR}" >/dev/null

helm template fuseki-operator "${CHART_DIR}" -n fuseki-system --include-crds >"${DEFAULT_RENDER}"
assert_contains "${DEFAULT_RENDER}" "kind: CustomResourceDefinition"
assert_contains "${DEFAULT_RENDER}" "kind: Deployment"
assert_contains "${DEFAULT_RENDER}" "name: fuseki-operator-controller-manager-metrics"

cat >"${OVERRIDE_VALUES}" <<'EOF'
image:
  tag: v0.1.0
  pullSecrets:
    - name: registry-creds
manager:
  extraArgs:
    - --zap-log-level=debug
    - --feature-gates=HelmValidation=true
podAnnotations:
  checksum/config: test-checksum
nodeSelector:
  kubernetes.io/os: linux
tolerations:
  - key: dedicated
    operator: Equal
    value: fuseki
    effect: NoSchedule
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/arch
              operator: In
              values:
                - amd64
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/fuseki-operator
metricsService:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
EOF

helm template fuseki-operator "${CHART_DIR}" -n fuseki-system -f "${OVERRIDE_VALUES}" >"${OVERRIDE_RENDER}"

assert_contains "${OVERRIDE_RENDER}" "imagePullSecrets:"
assert_contains "${OVERRIDE_RENDER}" "image: \"ghcr.io/larsw/k8s-fuseki-operator/controller:v0.1.0\""
assert_contains "${OVERRIDE_RENDER}" "- name: registry-creds"
assert_contains "${OVERRIDE_RENDER}" "- \"--zap-log-level=debug\""
assert_contains "${OVERRIDE_RENDER}" "- \"--feature-gates=HelmValidation=true\""
assert_contains "${OVERRIDE_RENDER}" "checksum/config: test-checksum"
assert_contains "${OVERRIDE_RENDER}" "kubernetes.io/os: linux"
assert_contains "${OVERRIDE_RENDER}" "key: dedicated"
assert_contains "${OVERRIDE_RENDER}" "value: fuseki"
assert_contains "${OVERRIDE_RENDER}" "kubernetes.io/arch"
assert_contains "${OVERRIDE_RENDER}" "eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/fuseki-operator"
assert_matches "${OVERRIDE_RENDER}" 'prometheus\.io/scrape: ("true"|true)'
assert_matches "${OVERRIDE_RENDER}" 'prometheus\.io/port: ("8080"|8080)'

echo "Helm chart render tests passed"