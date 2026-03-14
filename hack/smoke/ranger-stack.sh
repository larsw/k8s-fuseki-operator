#!/usr/bin/env bash

set -euo pipefail

container_tool="${CONTAINER_TOOL:-docker}"
stack_name="${RANGER_STACK_NAME:-fuseki-ranger-smoke}"
network_name="${RANGER_STACK_NETWORK:-rangernw}"
solr_container="${RANGER_SOLR_CONTAINER:-${stack_name}-solr}"
db_container="${RANGER_DB_CONTAINER:-${stack_name}-db}"
admin_container="${RANGER_ADMIN_CONTAINER:-${stack_name}-admin}"
admin_port="${RANGER_ADMIN_PORT:-16080}"

ranger_admin_image="${RANGER_ADMIN_IMAGE:-apache/ranger:2.8.0}"
ranger_db_image="${RANGER_DB_IMAGE:-apache/ranger-db:2.8.0}"
ranger_solr_image="${RANGER_SOLR_IMAGE:-apache/ranger-solr:2.8.0}"

ranger_db_user="${RANGER_DB_USER:-rangeradmin}"
ranger_db_password="${RANGER_DB_PASSWORD:-rangerR0cks!}"
ranger_username="${RANGER_USERNAME:-admin}"
ranger_password="${RANGER_PASSWORD:-rangerR0cks!}"
ranger_admin_url="${RANGER_ADMIN_URL:-http://127.0.0.1:${admin_port}/service}"

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "${command_name} is required for Ranger stack management" >&2
    exit 1
  fi
}

require_command "${container_tool}"
require_command curl

ensure_stack_namespace_available() {
  if [[ "${network_name}" != "rangernw" ]]; then
    return 0
  fi

  local existing
  existing=$("${container_tool}" ps --format '{{.Names}}' | grep -E '^(ranger-admin|ranger-db|ranger-solr)$' || true)
  if [[ -n "${existing}" ]]; then
    echo "An existing Ranger stack is already using the default internal namespace on this machine:" >&2
    printf '%s\n' "${existing}" >&2
    echo "Use that stack directly, or stop it before running ./hack/smoke/ranger-stack.sh." >&2
    exit 1
  fi
}

ensure_network() {
  if ! "${container_tool}" network inspect "${network_name}" >/dev/null 2>&1; then
    "${container_tool}" network create "${network_name}" >/dev/null
  fi
}

remove_container_if_present() {
  local container_name="$1"
  if "${container_tool}" container inspect "${container_name}" >/dev/null 2>&1; then
    "${container_tool}" rm -f "${container_name}" >/dev/null 2>&1 || true
  fi
}

probe_ranger_admin() {
  local probe_url="${ranger_admin_url}/public/v2/api/service"
  curl --silent --fail \
    --user "${ranger_username}:${ranger_password}" \
    --output /dev/null \
    "${probe_url}" >/dev/null 2>&1
}

wait_for_ranger_admin() {
  for attempt in $(seq 1 180); do
    if probe_ranger_admin; then
      return 0
    fi
    sleep 2
  done

  echo "Ranger admin did not become ready at ${ranger_admin_url}" >&2
  logs
  return 1
}

up() {
  down >/dev/null 2>&1 || true
  ensure_stack_namespace_available
  ensure_network

  "${container_tool}" run -d \
    --name "${solr_container}" \
    --network "${network_name}" \
    --network-alias ranger-solr \
    "${ranger_solr_image}" >/dev/null

  "${container_tool}" run -d \
    --name "${db_container}" \
    --network "${network_name}" \
    --network-alias ranger-db \
    -e POSTGRES_PASSWORD="${ranger_db_password}" \
    -e RANGER_DB_USER="${ranger_db_user}" \
    -e RANGER_DB_PASSWORD="${ranger_db_password}" \
    "${ranger_db_image}" >/dev/null

  "${container_tool}" run -d \
    --name "${admin_container}" \
    --network "${network_name}" \
    --network-alias ranger-admin \
    -p "${admin_port}:6080" \
    -e POSTGRES_PASSWORD="${ranger_db_password}" \
    -e RANGER_DB_USER="${ranger_db_user}" \
    -e RANGER_DB_PASSWORD="${ranger_db_password}" \
    "${ranger_admin_image}" >/dev/null

  wait_for_ranger_admin
}

down() {
  remove_container_if_present "${admin_container}"
  remove_container_if_present "${db_container}"
  remove_container_if_present "${solr_container}"
  if "${container_tool}" network inspect "${network_name}" >/dev/null 2>&1; then
    "${container_tool}" network rm "${network_name}" >/dev/null 2>&1 || true
  fi
}

logs() {
  if "${container_tool}" container inspect "${solr_container}" >/dev/null 2>&1; then
    echo "== ${solr_container} =="
    "${container_tool}" logs "${solr_container}" || true
  fi
  if "${container_tool}" container inspect "${db_container}" >/dev/null 2>&1; then
    echo "== ${db_container} =="
    "${container_tool}" logs "${db_container}" || true
  fi
  if "${container_tool}" container inspect "${admin_container}" >/dev/null 2>&1; then
    echo "== ${admin_container} =="
    "${container_tool}" logs "${admin_container}" || true
  fi
}

case "${1:-}" in
  up)
    up
    ;;
  down)
    down
    ;;
  logs)
    logs
    ;;
  *)
    echo "usage: $0 {up|down|logs}" >&2
    exit 1
    ;;
esac