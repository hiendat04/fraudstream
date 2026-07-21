#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/fraudstream-datahub-uv-cache}"

uv run --project "${ROOT_DIR}/datahub" --python 3.11 datahub docker quickstart --stop

