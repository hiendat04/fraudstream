#!/usr/bin/env bash
# Replaces a gateway key in Vault with a new random one. External Secrets copies it
# and its hash to the cluster within a minute; the old key stops working then.
# Check-and-set on the current version, so two rotations at once can't both win.
# The old version stays in Vault's history.
#   ./infra/vault/rotate-key.sh a2a/a2a-check
set -euo pipefail
cd "$(dirname "$0")/../.."
path=${1:?usage: rotate-key.sh <path under secret/>}
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/infra/vault/tls/ca.crt}"
: "${VAULT_TOKEN:=$(cat ~/.vault-token)}"
export VAULT_ADDR VAULT_CACERT VAULT_TOKEN

version=$(vault kv metadata get -mount=secret -format=json "$path" | jq -r .data.current_version)
openssl rand -hex 24 | jq -R --argjson v "$version" '{options: {cas: $v}, data: {key: .}}' | curl -sf --cacert "$VAULT_CACERT" \
  -H @<(printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN") -X POST --data @- "$VAULT_ADDR/v1/secret/data/$path" >/dev/null
echo "rotated  $path, was version $version"
