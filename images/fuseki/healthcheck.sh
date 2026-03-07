#!/usr/bin/env sh

set -eu

scheme="${FUSEKI_SERVER_SCHEME:-http}"
port="${FUSEKI_PORT:-3030}"
url="${scheme}://127.0.0.1:${port}/$/ping"

if [ "${scheme}" = "https" ]; then
	exec curl --silent --fail --insecure "${url}"
fi

exec curl --silent --fail "${url}"