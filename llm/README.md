# LLM Inference Platform

MLX-based fraud explanation service: serve a base model, sample training data
from the fraud Gold tables, fine-tune a LoRA adapter, and benchmark inference
throughput/latency. Runs locally on Apple Silicon via [mlx-lm](https://github.com/ml-explore/mlx-examples).

All commands below assume `cd llm` (uses `uv` for dependency/venv management).

## 1. Serve the model

```bash
./serve.sh
```

Starts `mlx_lm.server`. Env vars:

- `MODEL` — model repo id (default: `mlx-community/Qwen2.5-3B-Instruct-4bit`)
- `PORT` — server port (default: `8080`)

Any extra arguments are passed through to `mlx_lm.server`, e.g. to serve the
fine-tuned adapter on top of the base model:

```bash
./serve.sh --adapter-path models/adapters/fraud-explainer-v1
```

## 2. Sample fraud alerts from Postgres

`data/sample_alerts.py` pulls fraud-flagged rows from
`gold.feat_transaction_training` joined to `gold.fact_transactions`. Requires
Postgres running locally with the Gold layer populated.

DSN resolution: `FRAUDSTREAM_PG_DSN` env var, falling back to
`postgresql://fraudstream:fraudstream_local_password@localhost:5432/fraudstream`.

## 3. Build the training dataset

```bash
uv run python -m data.build_dataset --limit 300
```

Generates template-based prompt/completion pairs (no external LLM calls) and
writes `data/train.jsonl` / `data/valid.jsonl`. Flags:

- `--limit` — number of alert rows to fetch (default: `300`)
- `--valid-fraction` — fraction held out for validation (default: `0.15`)
- `--seed` — RNG seed for the shuffle/split (default: `42`)
- `--out-dir` — output directory for the JSONL files (default: `data`)

## 4. Fine-tune the LoRA adapter

```bash
./training/train_lora.sh
```

Runs `mlx_lm.lora` using `training/lora_config.yaml` (base model
`mlx-community/Qwen2.5-3B-Instruct-4bit`, 300 iters, batch size 4, LR 1e-5, 8
tuned layers). Produces `models/adapters/fraud-explainer-v1/` (gitignored).

Qualitative base-vs-tuned comparison on held-out prompts:

```bash
uv run python -m training.compare_outputs
```

Flags: `--base-model` (default: `mlx-community/Qwen2.5-3B-Instruct-4bit`),
`--adapter-path` (default: `models/adapters/fraud-explainer-v1`),
`--valid-path` (default: `data/valid.jsonl`), `--limit` (default: `5`).

## 5. Benchmark

With a server running (step 1 — pass `--adapter-path` to benchmark the tuned
model):

```bash
uv run python -m benchmark.run_benchmark --out benchmark/<name>.json
```

Sweeps concurrency levels and reports TTFT percentiles / throughput per
level. Flags:

- `--base-url` — model server URL (default: `http://localhost:8080`)
- `--prompts-path` — JSONL file of prompts to replay (default: `data/valid.jsonl`)
- `--concurrency-levels` — comma-separated levels (default: `1,2,4,8`)
- `--out` — output JSON path (default: `benchmark/results.json`)

`benchmark/*.json` outputs are gitignored (regenerable experiment output).

See [`docs/optimization/llm/inference_platform_optimization.md`](../docs/optimization/llm/inference_platform_optimization.md)
for the full measured comparison (quantization, LoRA overhead, context length).

## Tests

```bash
uv run pytest
```
