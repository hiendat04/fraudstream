#!/usr/bin/env bash
# Check that a UI is behind the gateway: HTTPS with a certificate from the local
# CA, refused without the password, answered with it. The burst test of
# check_gateway.sh is left out, because one page of a UI fires dozens of requests.
#   set -a && . ./.env && set +a
#   GATEWAY_ADDRESS=127.0.0.1 ./ci/check_ui.sh grafana.fraudstream.localhost /api/health
set -euo pipefail
cd "$(dirname "$0")/.."
. ci/gateway.sh

host=$1
path=${2:-/}
status() { gateway_curl -s -o /dev/null -w '%{http_code}' "$@" "https://$host$path"; }

without=$(status)
with=$(status -u "$GATEWAY_USER:$GATEWAY_PASSWORD")
if [ "$without" = 401 ] && [ "$with" = 200 ]; then
  echo "$host is behind the gateway"
else
  echo "$host: no password gave $without (want 401), the password gave $with (want 200)" >&2
  exit 1
fi
