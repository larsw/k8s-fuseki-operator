#!/usr/bin/env bash

set -euo pipefail

container_tool="${CONTAINER_TOOL:-docker}"
fuseki_image="${FUSEKI_IMAGE:-}"
ranger_admin_url="${RANGER_ADMIN_URL:-http://127.0.0.1:16080/service}"
ranger_username="${RANGER_USERNAME:-admin}"
ranger_password="${RANGER_PASSWORD:-rangerR0cks!}"
ranger_service_name="${RANGER_SERVICE_NAME:-fuseki-smoke}"
ranger_service_def_name="${RANGER_SERVICE_DEF_NAME:-fuseki-smoke}"
fuseki_port="${FUSEKI_SMOKE_PORT:-13031}"

if [[ -z "${fuseki_image}" ]]; then
  echo "FUSEKI_IMAGE must be set" >&2
  exit 1
fi

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "${command_name} is required for the Ranger smoke test" >&2
    exit 1
  fi
}

require_command "${container_tool}"
require_command curl
require_command python3

if [[ -n "${RANGER_ADMIN_CONTAINER_URL:-}" ]]; then
  ranger_container_url="${RANGER_ADMIN_CONTAINER_URL}"
else
  ranger_container_url="${ranger_admin_url/127.0.0.1/host.docker.internal}"
  ranger_container_url="${ranger_container_url/localhost/host.docker.internal}"
fi

container_id=""
ranger_probe_output="$(mktemp)"
bootstrap_log="$(mktemp)"
allow_output="$(mktemp)"
deny_output="$(mktemp)"

cleanup() {
  if [[ -n "${container_id}" ]]; then
    "${container_tool}" rm -f "${container_id}" >/dev/null 2>&1 || true
  fi
  rm -f "${ranger_probe_output}" "${bootstrap_log}" "${allow_output}" "${deny_output}"
}

show_failure_context() {
  if [[ -n "${container_id}" ]]; then
    "${container_tool}" logs "${container_id}" || true
  fi
  cat "${bootstrap_log}" || true
}

wait_for_fuseki_ping() {
  local url="$1"
  for attempt in $(seq 1 60); do
    if curl --silent --fail --output /dev/null "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

preflight_ranger_admin() {
  local probe_url="${ranger_admin_url}/public/v2/api/service"
  local status

  status=$(curl --silent --show-error --output "${ranger_probe_output}" --write-out '%{http_code}' \
    --user "${ranger_username}:${ranger_password}" \
    "${probe_url}" || true)

  case "${status}" in
    200)
      return 0
      ;;
    401|403)
      echo "Ranger admin credentials were rejected at ${probe_url}" >&2
      cat "${ranger_probe_output}" >&2 || true
      return 1
      ;;
    404)
      echo "Ranger admin URL ${ranger_admin_url} did not expose the public API endpoint ${probe_url}" >&2
      echo "If you are passing a base URL manually, it should usually end with /service." >&2
      cat "${ranger_probe_output}" >&2 || true
      return 1
      ;;
    000)
      echo "Ranger admin is not reachable at ${ranger_admin_url}" >&2
      return 1
      ;;
    *)
      echo "Unexpected Ranger admin response ${status} from ${probe_url}" >&2
      cat "${ranger_probe_output}" >&2 || true
      return 1
      ;;
  esac
}

trap cleanup EXIT

if ! preflight_ranger_admin; then
  exit 1
fi

RANGER_ADMIN_URL="${ranger_admin_url}" \
RANGER_USERNAME="${ranger_username}" \
RANGER_PASSWORD="${ranger_password}" \
RANGER_SERVICE_NAME="${ranger_service_name}" \
RANGER_SERVICE_DEF_NAME="${ranger_service_def_name}" \
python3 ./hack/smoke/bootstrap_ranger.py >"${bootstrap_log}" 2>&1 || {
  cat "${bootstrap_log}" >&2 || true
  echo "real Ranger bootstrap failed" >&2
  exit 1
}

container_id=$("${container_tool}" run -d \
  --add-host=host.docker.internal:host-gateway \
  -p "${fuseki_port}:3030" \
  -e SECURITY_PROFILE_AUTHORIZATION_MODE=Ranger \
  -e SECURITY_PROFILE_RANGER_ADMIN_URL="${ranger_container_url}" \
  -e SECURITY_PROFILE_RANGER_SERVICE_NAME="${ranger_service_name}" \
  -e SECURITY_PROFILE_RANGER_USERNAME="${ranger_username}" \
  -e SECURITY_PROFILE_RANGER_PASSWORD="${ranger_password}" \
  "${fuseki_image}" \
  sh -lc "cat > /tmp/smoke.ttl <<'EOF'
PREFIX fuseki: <http://jena.apache.org/fuseki#>
PREFIX ja: <http://jena.hpl.hp.com/2005/11/Assembler#>

[] a fuseki:Server ;
  fuseki:services ( <#smoke> <#roles> ) .

<#smoke> a fuseki:Service ;
  fuseki:name 'smoke' ;
  fuseki:serviceQuery 'query' ;
  fuseki:dataset [ a ja:MemoryDataset ] .

<#roles> a fuseki:Service ;
  fuseki:name 'roles' ;
  fuseki:serviceQuery 'query' ;
  fuseki:dataset [ a ja:MemoryDataset ] .
EOF
exec /opt/java/openjdk/bin/java -cp '/opt/fuseki/fuseki-server.jar:/opt/fuseki/fuseki-operator-launcher.jar:/opt/fuseki/fuseki-operator-deps/*' FusekiHttpsLauncher --config=/tmp/smoke.ttl --port=3030")

if ! wait_for_fuseki_ping "http://127.0.0.1:${fuseki_port}/$/ping"; then
  show_failure_context >&2
  echo "Fuseki Ranger smoke test failed during startup" >&2
  exit 1
fi

curl --silent --show-error --fail --get \
  -H 'X-Forwarded-User: alice' \
  -H 'X-Forwarded-Groups: fuseki-ops' \
  -H 'X-OIDC-Claim-department: data' \
  -H 'Accept: application/sparql-results+json' \
  --data-urlencode 'query=ASK {}' \
  "http://127.0.0.1:${fuseki_port}/smoke/query" >"${allow_output}" || {
    show_failure_context >&2
    echo "Fuseki Ranger smoke test failed during allowed query" >&2
    exit 1
  }

deny_status=$(curl --silent --show-error --output "${deny_output}" --write-out '%{http_code}' --get \
  -H 'X-Forwarded-User: alice' \
  -H 'X-Forwarded-Groups: fuseki-ops' \
  -H 'X-OIDC-Claim-department: finance' \
  -H 'Accept: application/sparql-results+json' \
  --data-urlencode 'query=ASK {}' \
  "http://127.0.0.1:${fuseki_port}/smoke/query")

if [[ "${deny_status}" != "403" ]]; then
  show_failure_context >&2
  echo "expected Ranger condition check to deny query, got HTTP ${deny_status}" >&2
  exit 1
fi

deny_status=$(curl --silent --show-error --output "${deny_output}" --write-out '%{http_code}' --get \
  -H 'X-Forwarded-User: carol' \
  -H 'Accept: application/sparql-results+json' \
  --data-urlencode 'query=ASK {}' \
  "http://127.0.0.1:${fuseki_port}/roles/query")

if [[ "${deny_status}" != "200" ]]; then
  show_failure_context >&2
  echo "expected Ranger role resolution to allow query for carol, got HTTP ${deny_status}" >&2
  exit 1
fi

deny_status=$(curl --silent --show-error --output "${deny_output}" --write-out '%{http_code}' --get \
  -H 'X-Forwarded-User: bob' \
  -H 'Accept: application/sparql-results+json' \
  --data-urlencode 'query=ASK {}' \
  "http://127.0.0.1:${fuseki_port}/roles/query")

if [[ "${deny_status}" != "403" ]]; then
  show_failure_context >&2
  echo "expected Ranger role resolution to deny query, got HTTP ${deny_status}" >&2
  exit 1
fi

echo "Fuseki Ranger smoke test passed"