#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/fraudstream-datahub-uv-cache}"
export DATAHUB_MAPPED_GMS_PORT="${DATAHUB_MAPPED_GMS_PORT:-18082}"

uv run --project "${ROOT_DIR}/datahub" --python 3.11 \
  datahub docker quickstart \
  --version v1.6.0

echo "DataHub UI: http://localhost:9002"
echo "DataHub metadata service: http://localhost:${DATAHUB_MAPPED_GMS_PORT}"
