#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: load.sh <database> [pattern...]

Load RDF files from /staging into a Fuseki database directory.
If no patterns are provided, common RDF file globs are searched.
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

database="$1"
shift

cd /staging 2>/dev/null || {
  echo "/staging is not available" >&2
  exit 1
}

patterns=("$@")
if [[ ${#patterns[@]} -eq 0 ]]; then
  patterns=("*.rdf" "*.rdf.gz" "*.ttl" "*.ttl.gz" "*.owl" "*.owl.gz" "*.nt" "*.nt.gz" "*.nq" "*.nq.gz")
fi

shopt -s nullglob
files=()
for pattern in "${patterns[@]}"; do
  for match in ${pattern}; do
    files+=("${match}")
  done
done
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No RDF files matched: ${patterns[*]}" >&2
  exit 1
fi

loader="${FUSEKI_HOME}/tdbloader"
if [[ "${TDB:-}" == "2" ]]; then
  loader="${FUSEKI_HOME}/tdbloader2"
fi

exec "${loader}" --loc="${FUSEKI_BASE}/databases/${database}" "${files[@]}"
