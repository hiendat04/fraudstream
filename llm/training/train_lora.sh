#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
uv run python -m mlx_lm.lora --config training/lora_config.yaml
