#!/usr/bin/env bash

set -euo pipefail

initialize_base() {
  mkdir -p "${FUSEKI_BASE}/databases"

  if [[ ! -f "${FUSEKI_BASE}/shiro.ini" ]]; then
    cp "${FUSEKI_HOME}/shiro.ini" "${FUSEKI_BASE}/shiro.ini"

    if [[ -z "${ADMIN_PASSWORD:-}" ]]; then
      ADMIN_PASSWORD="$(pwgen -s 20 1)"
      export ADMIN_PASSWORD
      echo "Generated Fuseki admin password: ${ADMIN_PASSWORD}"
    fi
  fi

  if [[ -n "${ADMIN_PASSWORD:-}" ]]; then
    export ADMIN_PASSWORD
    envsubst '${ADMIN_PASSWORD}' < "${FUSEKI_BASE}/shiro.ini" > "${FUSEKI_BASE}/shiro.ini.tmp"
    mv "${FUSEKI_BASE}/shiro.ini.tmp" "${FUSEKI_BASE}/shiro.ini"
  fi

  if [[ -d /fuseki-extra ]] && [[ ! -L "${FUSEKI_BASE}/extra" ]]; then
    ln -sfn /fuseki-extra "${FUSEKI_BASE}/extra"
  fi
}

wait_for_server() {
  for _ in $(seq 1 60); do
    if curl --silent --fail 'http://127.0.0.1:3030/$/ping' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  return 1
}

create_bootstrap_datasets() {
  local tdb_type="tdb"

  if [[ "${TDB:-}" == "2" ]]; then
    tdb_type="tdb2"
  fi

  while IFS='=' read -r name value; do
    [[ -z "${value}" ]] && continue
    curl --silent --show-error --fail \
      -u "admin:${ADMIN_PASSWORD:-}" \
      -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
      --data "dbName=${value}&dbType=${tdb_type}" \
      'http://127.0.0.1:3030/$/datasets' >/dev/null
  done < <(env | grep '^FUSEKI_DATASET_' || true)
}

initialize_base

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
