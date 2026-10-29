#!/usr/bin/env bash
# Starts Vault if it isn't running, then unseals it with the key in
# ~/.fraudstream/vault-init.json. Vault seals itself whenever its container
# restarts, after a Docker restart for one.
#   ./infra/vault/unseal.sh
set -euo pipefail
cd "$(dirname "$0")"
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/tls/ca.crt}"
export VAULT_ADDR VAULT_CACERT

# Vault's own file, which needs no secret; the other services are this project's too, not orphans.
COMPOSE_IGNORE_ORPHANS=1 docker compose -f compose.yml up -d --quiet-pull
# vault status exits 1 until the server answers, 2 while it is sealed.
for _ in $(seq 30); do
  status=0; vault status >/dev/null 2>&1 || status=$?
  [ "$status" = 1 ] || break
  sleep 1
done
if [ "$(vault status -format=json | jq -r .initialized)" != true ]; then
  echo "Vault is running, not yet initialized: ./infra/vault/init.sh"
  exit 0
fi
if [ "$(vault status -format=json | jq -r .sealed)" = false ]; then
  echo "Vault is already unsealed"
  exit 0
fi
# Through the API from stdin, so the key never shows in a command line.
jq '{key: .unseal_keys_b64[0]}' ~/.fraudstream/vault-init.json |
  curl -sf --cacert "$VAULT_CACERT" -X PUT --data @- "$VAULT_ADDR/v1/sys/unseal" >/dev/null
echo "Vault is unsealed"
