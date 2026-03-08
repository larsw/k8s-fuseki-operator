#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST_DIR="${DIST_DIR:-${ROOT_DIR}/dist}"

. "${ROOT_DIR}/release/metadata.env"

cd "${ROOT_DIR}"

RELEASE_COMMIT="${RELEASE_COMMIT:-$(git -C "${ROOT_DIR}" rev-parse --short=12 HEAD 2>/dev/null || echo none)}"
RELEASE_DATE="${RELEASE_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

checksum_files() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$@"
		return 0
	fi
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$@"
		return 0
	fi
	echo "missing sha256 checksum tool" >&2
	exit 1
}

mkdir -p "${DIST_DIR}"
rm -f "${DIST_DIR}"/*

bash "${ROOT_DIR}/hack/release/sync-metadata.sh"

helm package "${ROOT_DIR}/charts/fuseki-operator" --destination "${DIST_DIR}" >/dev/null
tar -czf "${DIST_DIR}/fuseki-operator-bundle-v${RELEASE_VERSION}.tar.gz" -C "${ROOT_DIR}" bundle bundle.Dockerfile

ldflags="-X fuseki-operator/pkg/version.Version=${RELEASE_VERSION} -X fuseki-operator/pkg/version.Commit=${RELEASE_COMMIT} -X fuseki-operator/pkg/version.Date=${RELEASE_DATE}"
for platform in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
	goos="${platform%/*}"
	goarch="${platform#*/}"
	output="${DIST_DIR}/fusekictl_${RELEASE_VERSION}_${goos}_${goarch}"
	CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" go build -ldflags "${ldflags}" -o "${output}" ./cmd/fusekictl
done

(
	cd "${DIST_DIR}"
	checksum_files * > checksums.txt
)