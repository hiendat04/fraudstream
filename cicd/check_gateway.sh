#!/usr/bin/env bash
# Check that one API is reachable only the way the gateway intends: HTTPS with a
# certificate from the local CA, and the right password.

#   . infra/vault/env.sh
#   GATEWAY_ADDRESS=127.0.0.1 ./cicd/check_gateway.sh inference.fraudstream.localhost
#   GATEWAY_ADDRESS=127.0.0.1 ./cicd/check_gateway.sh kagent.fraudstream.localhost /health

# Needs GATEWAY_USER and GATEWAY_PASSWORD. From the local computer, add GATEWAY_ADDRESS=127.0.0.1.
set -euo pipefail
cd "$(dirname "$0")/.."
. cicd/gateway.sh

host=$1
# The APIs answer /livez; the UIs have their own health path.
path=${2:-/livez}
url="https://$host$path"

expect() {
  if [ "$3" = "$2" ]; then
    echo "  ok    $1: $3"
  else
    echo "  FAIL  $1: got '$3', wanted '$2'" >&2
    exit 1
  fi
}

status() { gateway_curl -s -o /dev/null -w '%{http_code}' "$@" "$url"; }

expect "plain HTTP is redirected, method kept" "308 https://$host:443$path" \
  "$(gateway_curl -s -o /dev/null -w '%{http_code} %{redirect_url}' "http://$host$path")"
expect "no password" 401 "$(status)"
expect "wrong password" 401 "$(status -u "$GATEWAY_USER:wrong")"
expect "right password, certificate trusted" 200 "$(status -u "$GATEWAY_USER:$GATEWAY_PASSWORD")"

echo "$host is behind the gateway"
