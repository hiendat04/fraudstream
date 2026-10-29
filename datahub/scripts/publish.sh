#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/fraudstream-datahub-uv-cache}"
export FRAUDSTREAM_POSTGRES_HOST_PORT="${FRAUDSTREAM_POSTGRES_HOST_PORT:-localhost:5432}"
export FRAUDSTREAM_POSTGRES_DATABASE="${FRAUDSTREAM_POSTGRES_DATABASE:-fraudstream}"
# The login comes from Vault unless it is set; needs `vault login -method=userpass username=operator`.
export VAULT_ADDR="${VAULT_ADDR:-https://localhost:8200}" VAULT_CACERT="${VAULT_CACERT:-${ROOT_DIR}/infra/vault/tls/ca.crt}"
: "${FRAUDSTREAM_POSTGRES_USER:=$(vault kv get -mount=secret -field=login platform/postgres)}"
: "${FRAUDSTREAM_POSTGRES_PASSWORD:=$(vault kv get -mount=secret -field=password platform/postgres)}"
export FRAUDSTREAM_POSTGRES_USER FRAUDSTREAM_POSTGRES_PASSWORD
export DATAHUB_GMS_URL="${DATAHUB_GMS_URL:-http://localhost:18087}"
export DATAHUB_UI_URL="${DATAHUB_UI_URL:-http://localhost:9002}"
export DATAHUB_TOKEN="${DATAHUB_TOKEN:-}"

uv run --project "${ROOT_DIR}/datahub" --python 3.11 \
  datahub ingest -c "${ROOT_DIR}/datahub/recipes/postgres.dhub.yaml"

publish_args=(
  --project-root "${ROOT_DIR}"
  --server "${DATAHUB_GMS_URL}"
  --ui-url "${DATAHUB_UI_URL}"
)
if [[ -n "${DATAHUB_TOKEN}" ]]; then
  publish_args+=(--token "${DATAHUB_TOKEN}")
fi

uv run --project "${ROOT_DIR}/datahub" --python 3.11 \
  fraudstream-publish-governance "${publish_args[@]}"
