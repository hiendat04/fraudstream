#!/usr/bin/env bash
# Checks Vault and everything that reads from it. Each section checks one kind of
# reader; with no section given, all of them run. Secret values are never printed,
# only whether they are set and whether two copies hash the same.
#   ./infra/vault/check.sh [section]...
#   ./infra/vault/check.sh server        Vault is up, unsealed, on Raft, over TLS, with an audit log
# Needs VAULT_ADDR, VAULT_CACERT and a token from `vault login -method=userpass username=operator`.
set -euo pipefail
cd "$(dirname "$0")/../.."

failed=0
expect() {
  if [ "$3" = "$2" ]; then
    echo "  ok    $1: $3"
  else
    echo "  FAIL  $1: got '$3', wanted '$2'"
    failed=1
  fi
}

server() {
  echo "==> server"
  local status
  status=$(vault status -format=json 2>/dev/null || true)
  expect "initialized" true "$(jq -r '.initialized // empty' <<<"$status")"
  expect "unsealed" false "$(jq -r 'if .sealed == null then "" else .sealed end' <<<"$status")"
  expect "storage" raft "$(jq -r '.storage_type // empty' <<<"$status")"
  expect "KV mount secret/ at version" 2 \
    "$(vault secrets list -format=json 2>/dev/null | jq -r '."secret/".options.version // empty')"
  expect "audit device" file \
    "$(vault audit list -format=json 2>/dev/null | jq -r '."file/".type // empty')"
  expect "login for people" userpass \
    "$(vault auth list -format=json 2>/dev/null | jq -r '."userpass/".type // empty')"
  expect "health over HTTPS" 200 \
    "$(curl --cacert "$VAULT_CACERT" -s -o /dev/null -w '%{http_code}' "$VAULT_ADDR/v1/sys/health" || true)"
  expect "certificate issuer" "FraudStream Vault CA" \
    "$(openssl s_client -connect localhost:8200 -servername localhost </dev/null 2>/dev/null \
      | openssl x509 -noout -issuer -nameopt multiline 2>/dev/null | sed -n 's/^ *commonName *= *//p')"
}

: "${VAULT_ADDR:?set VAULT_ADDR=https://localhost:8200}" "${VAULT_CACERT:?set VAULT_CACERT to infra/vault/tls/ca.crt}"
sections=("$@")
[ ${#sections[@]} -gt 0 ] || sections=(server)
for section in "${sections[@]}"; do
  case "$section" in
    server) server ;;
    *) echo "unknown section: $section" >&2; exit 2 ;;
  esac
done
exit "$failed"
