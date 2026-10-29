#!/usr/bin/env bash
# Runs docker compose with the stores', the gateway's, CI's and Airflow's secrets
# read from Vault into this script's environment, where Compose fills them in,
# and Jenkins' AppRole login.
# docker-compose.yml has no defaults for them, so Compose refuses to run without
# this script. Needs Vault unsealed and `vault login -method=userpass username=operator`.
#   ./infra/vault/compose.sh up -d
#   ./infra/vault/compose.sh --profile orchestration up --build -d
#   ./infra/vault/compose.sh ps
set -euo pipefail
cd "$(dirname "$0")/../.."
. infra/vault/env.sh

# Jenkins' own Vault login, a fresh secret id each run: Jenkins reads every other
# credential from Vault with it when a build uses one.
VAULT_ROLE_ID=$(vault read -field=role_id auth/approle/role/jenkins/role-id)
VAULT_SECRET_ID=$(vault write -f -field=secret_id auth/approle/role/jenkins/secret-id)
export VAULT_ROLE_ID VAULT_SECRET_ID

exec docker compose "$@"
