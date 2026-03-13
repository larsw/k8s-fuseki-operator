#!/usr/bin/env bash

set -euo pipefail

initialize_base() {
  mkdir -p "${FUSEKI_BASE}/databases"

  if [[ -n "${ADMIN_PASSWORD:-}" ]] && [[ ! -f "${FUSEKI_BASE}/shiro.ini" ]]; then
    cp "${FUSEKI_HOME}/shiro.ini" "${FUSEKI_BASE}/shiro.ini"
  fi

  if [[ -n "${ADMIN_PASSWORD:-}" ]] && [[ -f "${FUSEKI_BASE}/shiro.ini" ]]; then
    export ADMIN_PASSWORD
    envsubst '${ADMIN_PASSWORD}' < "${FUSEKI_BASE}/shiro.ini" > "${FUSEKI_BASE}/shiro.ini.tmp"
    mv "${FUSEKI_BASE}/shiro.ini.tmp" "${FUSEKI_BASE}/shiro.ini"
  fi

  if [[ -d /fuseki-extra ]] && [[ ! -L "${FUSEKI_BASE}/extra" ]]; then
    ln -sfn /fuseki-extra "${FUSEKI_BASE}/extra"
  fi
}

prepare_local_authorization_runtime() {
  local dataset_root="${FUSEKI_DATASET_CONFIG_DIR:-/fuseki-extra/dataset-config}"
  local authorization_dir="${FUSEKI_AUTHORIZATION_DIR:-/fuseki-extra/authorization}"
  local authorization_index="${FUSEKI_AUTHORIZATION_INDEX:-${authorization_dir}/policies.index}"

  mkdir -p "${authorization_dir}"
  : > "${authorization_index}"

  if [[ ! -d "${dataset_root}" ]]; then
    return 0
  fi

  shopt -s nullglob
  for dataset_dir in "${dataset_root}"/*; do
    [[ -d "${dataset_dir}" ]] || continue
    local dataset_name
    dataset_name="$(basename "${dataset_dir}")"

    if [[ -f "${dataset_dir}/security-policies.missing" ]]; then
      echo "Dataset ${dataset_name} has unresolved SecurityPolicy references; refusing to start in Local authorization mode" >&2
      exit 1
    fi

    if [[ -f "${dataset_dir}/security-policies.json" ]]; then
      printf '%s=%s\n' "${dataset_name}" "${dataset_dir}/security-policies.json" >> "${authorization_index}"
    fi
  done
  shopt -u nullglob
}

probe_ranger_admin() {
  local admin_url="${SECURITY_PROFILE_RANGER_ADMIN_URL:-}"
  local curl_args=(--silent --show-error --output /dev/null --max-time 10)

  if [[ -z "${admin_url}" ]]; then
    echo "SECURITY_PROFILE_RANGER_ADMIN_URL must be set for Ranger authorization mode" >&2
    exit 1
  fi

  if [[ -n "${SECURITY_PROFILE_RANGER_USERNAME:-}" ]] || [[ -n "${SECURITY_PROFILE_RANGER_PASSWORD:-}" ]]; then
    curl_args+=(-u "${SECURITY_PROFILE_RANGER_USERNAME:-}:${SECURITY_PROFILE_RANGER_PASSWORD:-}")
  fi

  curl "${curl_args[@]}" "${admin_url}"
}

prepare_ranger_authorization_runtime() {
  local dataset_root="${FUSEKI_DATASET_CONFIG_DIR:-/fuseki-extra/dataset-config}"
  local authorization_dir="${FUSEKI_AUTHORIZATION_DIR:-/fuseki-extra/authorization}"
  local ranger_config="${SECURITY_PROFILE_RANGER_CONFIG:-${authorization_dir}/ranger.properties}"

  if [[ -d "${dataset_root}" ]] && find "${dataset_root}" -mindepth 2 -maxdepth 2 -name security-policies.json | grep -q .; then
    echo "Ranger authorization mode cannot be combined with local dataset SecurityPolicy bundles" >&2
    exit 1
  fi

  mkdir -p "${authorization_dir}"
  cat > "${ranger_config}" <<EOF
ranger.adminURL=${SECURITY_PROFILE_RANGER_ADMIN_URL:-}
ranger.serviceName=${SECURITY_PROFILE_RANGER_SERVICE_NAME:-}
ranger.authSecretRef=${SECURITY_PROFILE_RANGER_AUTH_SECRET:-}
ranger.pollInterval=${SECURITY_PROFILE_RANGER_POLL_INTERVAL:-30s}
EOF

  probe_ranger_admin
}

prepare_authorization_runtime() {
  local authorization_mode="${SECURITY_PROFILE_AUTHORIZATION_MODE:-Local}"
  local fail_closed="${FUSEKI_AUTHORIZATION_FAIL_CLOSED:-true}"

  case "${authorization_mode}" in
    Local)
      prepare_local_authorization_runtime
      ;;
    Ranger)
      prepare_ranger_authorization_runtime
      ;;
    *)
      if [[ "${fail_closed}" == "true" ]]; then
        echo "Unsupported authorization mode: ${authorization_mode}" >&2
        exit 1
      fi
      ;;
  esac
}

fuseki_local_url() {
  local scheme="${FUSEKI_SERVER_SCHEME:-http}"
  local port="${FUSEKI_PORT:-3030}"
  printf '%s://127.0.0.1:%s' "${scheme}" "${port}"
}

curl_fuseki_local() {
  local base_url
  base_url="$(fuseki_local_url)"

  if [[ "${base_url}" == https://* ]]; then
    curl --silent --show-error --fail --insecure "$@"
    return
  fi

  curl --silent --show-error --fail "$@"
}

wait_for_server() {
  local ping_url
  ping_url="$(fuseki_local_url)/$/ping"

  for _ in $(seq 1 60); do
    if curl_fuseki_local "${ping_url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  return 1
}

create_bootstrap_datasets() {
  local tdb_type="tdb"
  local datasets_url
  datasets_url="$(fuseki_local_url)/$/datasets"

  if [[ "${TDB:-}" == "2" ]]; then
    tdb_type="tdb2"
  fi

  while IFS='=' read -r name value; do
    [[ -z "${value}" ]] && continue
    curl_fuseki_local \
      -u "admin:${ADMIN_PASSWORD:-}" \
      -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
      --data "dbName=${value}&dbType=${tdb_type}" \
      "${datasets_url}" >/dev/null
  done < <(env | grep '^FUSEKI_DATASET_' || true)
}

initialize_base
prepare_authorization_runtime

"$@" &
server_pid=$!
trap 'kill ${server_pid} >/dev/null 2>&1 || true' INT TERM

if wait_for_server; then
  create_bootstrap_datasets
else
  echo "Fuseki failed to become ready during bootstrap" >&2
  exit 1
fi

wait "${server_pid}"
