#!/usr/bin/env bash
# Check that one API is reachable only the way the gateway intends: HTTPS with a
# certificate from the local CA, the right password, and a rate limit.

#   set -a && . ./.env && set +a
#   GATEWAY_ADDRESS=127.0.0.1 ./ci/check_gateway.sh inference.fraudstream.localhost

# Needs GATEWAY_USER and GATEWAY_PASSWORD. From the local computer, add GATEWAY_ADDRESS=127.0.0.1.
set -euo pipefail
cd "$(dirname "$0")/.."
. ci/gateway.sh

host=$1
url="https://$host/livez"

expect() {
  if [ "$3" = "$2" ]; then
    echo "  ok    $1: $3"
  else
    echo "  FAIL  $1: got '$3', wanted '$2'" >&2
    exit 1
  fi
}

status() { gateway_curl -s -o /dev/null -w '%{http_code}' "$@" "$url"; }

# Many more requests at once than the limit allows: some must get through, some must be refused.
burst() {
  seq 200 | xargs -P 50 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
    --connect-to "::$gateway_address:" --cacert "$gateway_ca" -u "$1" "$url" | sort | uniq -c
}

expect "plain HTTP is redirected, method kept" "308 https://$host:443/livez" \
  "$(gateway_curl -s -o /dev/null -w '%{http_code} %{redirect_url}' "http://$host/livez")"
expect "no password" 401 "$(status)"
expect "wrong password" 401 "$(status -u "$GATEWAY_USER:wrong")"
expect "right password, certificate trusted" 200 "$(status -u "$GATEWAY_USER:$GATEWAY_PASSWORD")"

answers=$(burst "$GATEWAY_USER:$GATEWAY_PASSWORD")
echo "$answers" | sed 's/^/        burst: /'
expect "a burst is partly let through" yes "$(echo "$answers" | grep -qE ' 200$' && echo yes || echo no)"
expect "a burst is partly refused" yes "$(echo "$answers" | grep -qE ' 429$' && echo yes || echo no)"

sleep 2
answers=$(burst "$GATEWAY_USER:wrong")
expect "guessing passwords is rate limited too" yes "$(echo "$answers" | grep -qE ' 429$' && echo yes || echo no)"

sleep 2
echo "$host is behind the gateway"
