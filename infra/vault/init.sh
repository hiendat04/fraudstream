#!/usr/bin/env bash
# Initializes Vault once: one unseal key and the root token, kept outside the repo
# in ~/.fraudstream/vault-init.json; then unseals it, turns on the audit log, the
# KV store secret/ and the login for people, and makes the user operator.
# Safe to run again: it only does what is still missing.
#   ./infra/vault/init.sh
#   ./infra/vault/init.sh --new-password     set a new password for operator, and unlock it; Vault keeps only its hash
set -euo pipefail
cd "$(dirname "$0")"
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/tls/ca.crt}"
export VAULT_ADDR VAULT_CACERT
init_file=~/.fraudstream/vault-init.json

if [ "$(vault status -format=json | jq -r .initialized)" != true ]; then
  mkdir -p ~/.fraudstream
  (umask 077 && vault operator init -key-shares=1 -key-threshold=1 -format=json >"$init_file")
  echo "==> the unseal key and root token are in $init_file"
fi
./unseal.sh

VAULT_TOKEN=$(jq -r .root_token "$init_file")
export VAULT_TOKEN
# Each step only when missing, so a run that stopped half way is finished by the next.
vault audit list -format=json | jq -e '."file/"' >/dev/null || vault audit enable file file_path=stdout >/dev/null
vault secrets list -format=json | jq -e '."secret/"' >/dev/null || vault secrets enable -path=secret kv-v2 >/dev/null
vault auth list -format=json | jq -e '."userpass/"' >/dev/null || vault auth enable userpass >/dev/null
vault policy write operator policies/operator.hcl >/dev/null
if [ "${1:-}" != --new-password ] && vault read auth/userpass/users/operator >/dev/null 2>&1; then
  echo "==> the user operator exists: vault login -method=userpass username=operator"
  echo "    forgot its password? ./infra/vault/init.sh --new-password"
  exit 0
fi

read -rsp "A password for the Vault user operator: " password; echo
read -rsp "The same password again: " again; echo
[ "$password" = "$again" ] || { echo "the passwords differ" >&2; exit 1; }
# Through the API: the CLI's password=- keeps the newline a here-string adds. The
# token goes in a header from a file descriptor, so neither shows in a command line.
jq -n --arg password "$password" '{password: $password, token_policies: "operator"}' |
  curl -sf --cacert "$VAULT_CACERT" -H @<(printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN") \
    -X POST --data @- "$VAULT_ADDR/v1/auth/userpass/users/operator" >/dev/null
# Failed logins lock the user for 15 minutes; a new password unlocks it.
accessor=$(vault auth list -format=json | jq -r '."userpass/".accessor')
vault write -f "sys/locked-users/$accessor/unlock/operator" >/dev/null
echo "==> Vault is ready: vault login -method=userpass username=operator"
