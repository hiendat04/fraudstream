#!/usr/bin/env bash
# Unseals Vault with the key in ~/.fraudstream/vault-init.json. Vault seals itself
# whenever its container restarts, after a Docker restart for one.
#   ./infra/vault/unseal.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/tls/ca.crt}"
export VAULT_ADDR VAULT_CACERT

if [ "$(vault status -format=json | jq -r .sealed)" = false ]; then
  echo "Vault is already unsealed"
  exit 0
fi
# Through the API from stdin, so the key never shows in a command line.
jq '{key: .unseal_keys_b64[0]}' ~/.fraudstream/vault-init.json |
  curl -sf --cacert "$VAULT_CACERT" -X PUT --data @- "$VAULT_ADDR/v1/sys/unseal" >/dev/null
echo "Vault is unsealed"
