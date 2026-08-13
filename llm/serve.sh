#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

MODEL="${MODEL:-mlx-community/Qwen2.5-3B-Instruct-4bit}"
PORT="${PORT:-8080}"

uv run python -m mlx_lm.server --model "$MODEL" --port "$PORT" "$@"
