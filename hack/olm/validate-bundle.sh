#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUNDLE_DIR="${ROOT_DIR}/bundle"
CSV_FILE="${BUNDLE_DIR}/manifests/fuseki-operator.clusterserviceversion.yaml"
ANNOTATIONS_FILE="${BUNDLE_DIR}/metadata/annotations.yaml"

assert_file() {
	local path=$1
	[[ -f "${path}" ]] || {
		echo "missing required bundle file: ${path}" >&2
		exit 1
	}
}

assert_contains() {
	local path=$1
	local needle=$2
	if ! grep -Fq -- "${needle}" "${path}"; then
		echo "expected ${path} to contain: ${needle}" >&2
		exit 1
	fi
}

assert_file "${CSV_FILE}"
assert_file "${ANNOTATIONS_FILE}"
assert_file "${ROOT_DIR}/bundle.Dockerfile"

crd_count=$(find "${BUNDLE_DIR}/manifests" -maxdepth 1 -type f -name 'fuseki.apache.org_*.yaml' | wc -l | tr -d ' ')
if [[ "${crd_count}" != "9" ]]; then
	echo "expected 9 CRD manifests in ${BUNDLE_DIR}/manifests, found ${crd_count}" >&2
	exit 1
fi

assert_contains "${ANNOTATIONS_FILE}" "operators.operatorframework.io.bundle.package.v1: fuseki-operator"
assert_contains "${ANNOTATIONS_FILE}" "operators.operatorframework.io.bundle.channels.v1: alpha"
assert_contains "${CSV_FILE}" "name: fuseki-operator.v0.1.0"
assert_contains "${CSV_FILE}" "displayName: Fuseki Operator"
assert_contains "${CSV_FILE}" "containerImage: ghcr.io/example/fuseki-operator/controller:dev"

for owned_crd in \
	backuppolicies.fuseki.apache.org \
	datasets.fuseki.apache.org \
	endpoints.fuseki.apache.org \
	fusekiclusters.fuseki.apache.org \
	fusekiservers.fuseki.apache.org \
	fusekiuis.fuseki.apache.org \
	rdfdeltaservers.fuseki.apache.org \
	restorerequests.fuseki.apache.org \
	securityprofiles.fuseki.apache.org; do
	assert_contains "${CSV_FILE}" "name: ${owned_crd}"
done

if command -v operator-sdk >/dev/null 2>&1; then
	operator-sdk bundle validate "${BUNDLE_DIR}"
else
	echo "operator-sdk not found in PATH; skipped upstream bundle validation" >&2
fi

echo "OLM bundle validation passed"