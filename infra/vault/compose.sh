#!/usr/bin/env bash
# Runs docker compose with the stores', the gateway's, CI's and Airflow's secrets
# read from Vault into this script's environment, where Compose fills them in.
# docker-compose.yml has no defaults for them, so Compose refuses to run without
# this script. Needs Vault unsealed and `vault login -method=userpass username=operator`.
#   ./infra/vault/compose.sh up -d
#   ./infra/vault/compose.sh --profile orchestration up --build -d
#   ./infra/vault/compose.sh ps
set -euo pipefail
cd "$(dirname "$0")/../.."
. infra/vault/env.sh

exec docker compose "$@"
