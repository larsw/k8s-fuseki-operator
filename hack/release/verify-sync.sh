#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
FILES=(
	"charts/fuseki-operator/Chart.yaml"
	"charts/fuseki-operator/values.yaml"
	"bundle/metadata/annotations.yaml"
	"bundle/manifests/fuseki-operator.clusterserviceversion.yaml"
)

cleanup() {
	rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

for file in "${FILES[@]}"; do
	mkdir -p "${TMP_DIR}/$(dirname "${file}")"
	cp "${ROOT_DIR}/${file}" "${TMP_DIR}/${file}"
done

bash "${ROOT_DIR}/hack/release/sync-metadata.sh"

for file in "${FILES[@]}"; do
	if ! cmp -s "${ROOT_DIR}/${file}" "${TMP_DIR}/${file}"; then
		echo "release metadata is out of sync: ${file}" >&2
		echo "run: make release-sync" >&2
		exit 1
	fi
done