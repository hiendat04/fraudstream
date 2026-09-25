#!/usr/bin/env bash
# Wait until an API behind the gateway reports the version it was just deployed as.
#   ./ci/wait_for_version.sh inference.fraudstream.localhost 9f789a7
# Needs GATEWAY_USER and GATEWAY_PASSWORD. From the local computer, add GATEWAY_ADDRESS=127.0.0.1.
set -euo pipefail
cd "$(dirname "$0")/.."
. ci/gateway.sh

host=$1
expected=$2

answer=""
# The new pod is Ready before NGINX has dropped the old one from its upstreams.
for _ in $(seq 30); do
  answer=$(gateway_curl -s -o /dev/null --max-time 5 -w '%{http_code} %header{x-app-version}' \
    -u "$GATEWAY_USER:$GATEWAY_PASSWORD" "https://$host/livez" || true)
  if [ "$answer" = "200 $expected" ]; then
    echo "$host is serving $expected"
    exit 0
  fi
  sleep 2
done

echo "$host answered '$answer', wanted '200 $expected'" >&2
exit 1
