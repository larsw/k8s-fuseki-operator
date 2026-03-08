#!/usr/bin/env bash

set -euo pipefail

VERSION="${OPERATOR_SDK_VERSION:-v1.42.0}"
INSTALL_DIR="${OPERATOR_SDK_INSTALL_DIR:-${HOME}/.local/bin}"
ARCH="$(case "$(uname -m)" in
	x86_64) printf amd64 ;;
	aarch64|arm64) printf arm64 ;;
	*) printf '%s' "$(uname -m)" ;;
esac)"
OS="$(uname | tr '[:upper:]' '[:lower:]')"
TMP_DIR="$(mktemp -d)"
BIN_NAME="operator-sdk_${OS}_${ARCH}"
BASE_URL="https://github.com/operator-framework/operator-sdk/releases/download/${VERSION}"

cleanup() {
	rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

curl -fsSL -o "${TMP_DIR}/${BIN_NAME}" "${BASE_URL}/${BIN_NAME}"
curl -fsSL -o "${TMP_DIR}/checksums.txt" "${BASE_URL}/checksums.txt"

checksum="$(grep "${BIN_NAME}$" "${TMP_DIR}/checksums.txt" | awk '{print $1}')"
if [[ -z "${checksum}" ]]; then
	echo "failed to resolve checksum for ${BIN_NAME}" >&2
	exit 1
fi

printf '%s  %s\n' "${checksum}" "${TMP_DIR}/${BIN_NAME}" | sha256sum -c -
mkdir -p "${INSTALL_DIR}"
install -m 0755 "${TMP_DIR}/${BIN_NAME}" "${INSTALL_DIR}/operator-sdk"
"${INSTALL_DIR}/operator-sdk" version