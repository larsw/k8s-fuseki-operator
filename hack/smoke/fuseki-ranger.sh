#!/usr/bin/env bash

set -euo pipefail

container_tool="${CONTAINER_TOOL:-docker}"
fuseki_image="${FUSEKI_IMAGE:-}"
mock_port="${RANGER_SMOKE_PORT:-18181}"
fuseki_port="${FUSEKI_SMOKE_PORT:-13031}"

if [[ -z "${fuseki_image}" ]]; then
  echo "FUSEKI_IMAGE must be set" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for the Ranger smoke test" >&2
  exit 1
fi

container_id=""
mock_pid=""
mock_log="$(mktemp)"
allow_output="$(mktemp)"
deny_output="$(mktemp)"

cleanup() {
  if [[ -n "${container_id}" ]]; then
    "${container_tool}" rm -f "${container_id}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${mock_pid}" ]]; then
    kill "${mock_pid}" >/dev/null 2>&1 || true
    wait "${mock_pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${mock_log}" "${allow_output}" "${deny_output}"
}

show_failure_context() {
  if [[ -n "${container_id}" ]]; then
    "${container_tool}" logs "${container_id}" || true
  fi
  cat "${mock_log}" || true
}

wait_for_http_200() {
  local url="$1"
  for attempt in $(seq 1 60); do
    if curl --silent --show-error --fail --output /dev/null "${url}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

trap cleanup EXIT

python3 ./hack/smoke/mock_ranger_server.py "${mock_port}" >"${mock_log}" 2>&1 &
mock_pid=$!

if ! wait_for_http_200 "http://127.0.0.1:${mock_port}/"; then
  cat "${mock_log}" >&2 || true
  echo "mock Ranger server failed to start" >&2
  exit 1
fi

container_id=$("${container_tool}" run -d \
  --add-host=host.docker.internal:host-gateway \
  -p "${fuseki_port}:3030" \
  -e SECURITY_PROFILE_AUTHORIZATION_MODE=Ranger \
  -e SECURITY_PROFILE_RANGER_ADMIN_URL="http://host.docker.internal:${mock_port}" \
  -e SECURITY_PROFILE_RANGER_SERVICE_NAME=fuseki-default \
  -e SECURITY_PROFILE_RANGER_USERNAME=admin \
  -e SECURITY_PROFILE_RANGER_PASSWORD=secret \
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

if ! wait_for_http_200 "http://127.0.0.1:${fuseki_port}/$/ping"; then
  show_failure_context >&2
  echo "Fuseki Ranger smoke test failed during startup" >&2
  exit 1
fi

curl --silent --show-error --fail --get \
  -H 'X-Forwarded-User: alice' \
  -H 'X-Forwarded-Groups: ops' \
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
  -H 'X-Forwarded-Groups: ops' \
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