#!/usr/bin/env bash
# Makes a key in Vault, once: 48 random hex characters at secret/<path>, in the
# field key or in each field named. Written with check-and-set 0, so a key that
# exists is never changed. Prints made or kept, never the key.
#   ./infra/vault/new-key.sh llm/benchmark
#   ./infra/vault/new-key.sh agents/substrate-rustfs access_key secret_key
set -euo pipefail
cd "$(dirname "$0")/../.."
path=${1:?usage: new-key.sh <path under secret/> [field]...}
fields=("${@:2}")
[ ${#fields[@]} -gt 0 ] || fields=(key)
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/infra/vault/tls/ca.crt}"
: "${VAULT_TOKEN:=$(cat ~/.vault-token)}"
export VAULT_ADDR VAULT_CACERT VAULT_TOKEN

if vault kv metadata get -mount=secret "$path" >/dev/null 2>&1; then
  echo "kept  $path"
  exit 0
fi

for field in "${fields[@]}"; do printf '%s\t%s\n' "$field" "$(openssl rand -hex 24)"; done |
  jq -Rn '{options: {cas: 0}, data: ([inputs | split("\t") | {(.[0]): .[1]}] | add)}' | curl -sf --cacert "$VAULT_CACERT" \
  -H @<(printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN") -X POST --data @- "$VAULT_ADDR/v1/secret/data/$path" >/dev/null
echo "made  $path"
