#!/usr/bin/env bash
# Wait until an API behind the ingress reports the version it was just deployed as.
#   ./ci/wait_for_version.sh inference.localhost 9f789a7
#   INGRESS_URL=http://localhost ./ci/wait_for_version.sh inference.localhost 9f789a7   # from the host
set -euo pipefail

host=$1
expected=$2
ingress=${INGRESS_URL:-http://fraudstream-control-plane}

answer=""
# The new pod is Ready before NGINX has dropped the old one from its upstreams.
for _ in $(seq 30); do
  answer=$(curl -s -o /dev/null --max-time 5 -w '%{http_code} %header{x-app-version}' \
    -H "Host: $host" "$ingress/livez" || true)
  if [ "$answer" = "200 $expected" ]; then
    echo "$host is serving $expected"
    exit 0
  fi
  sleep 2
done

echo "$host answered '$answer', wanted '200 $expected'" >&2
exit 1
