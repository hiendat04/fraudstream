# LLM Inference Platform for Fraud Explanation — Design

## Status
Approved by user 2026-08-13. Scope limited to model serving, custom model (LoRA), and benchmarking/optimization. Gateway/routing and agent/tool-calling orchestration are explicitly out of scope — deferred to a future spec.

## Motivation

FraudStream today is a pure data-engineering pipeline (Spark/Flink/Kafka/Airflow/Iceberg, all local Docker) — no model training, scoring API, or LLM component exists. This spec adds an applied-AI-engineering component: a locally-served LLM that turns the pipeline's already-computed fraud alerts and features into human-readable investigation explanations for an analyst.

The goal is a Data Engineering + MLOps + Applied AI Engineering deliverable focused on **deploying and serving** a model, not training one from scratch. The three concrete outcomes are:
1. Deploy an LLM inference platform.
2. Set up a custom model (base model + light LoRA fine-tuning).
3. Benchmark the model server and optimize it.

## Why not the literal llm-d/vLLM/Kubernetes stack

The llm-d quickstart (the starting reference point for this work) assumes a Kubernetes cluster with NVIDIA GPUs. The actual target hardware is a local **Apple M1 Pro Mac, 32GB RAM, no NVIDIA GPU**. vLLM's CPU backend targets x86 AVX-512 and has no Metal (Apple GPU) acceleration path, so running the literal llm-d stack here would mean no GPU acceleration and a weak benchmarking/optimization story.

Instead, this design uses **MLX** (Apple's native ML framework), which gets real Metal GPU acceleration on this hardware and has first-class local LoRA fine-tuning support — keeping the entire pipeline (serve + fine-tune + benchmark) local and free, with Hugging Face still as the model source (`mlx-community` hosts pre-converted MLX builds of standard instruct models). This preserves the same architectural pattern (model server behind an OpenAI-compatible API, later a gateway) while fitting the hardware that's actually available, rather than cargo-culting a tutorial built for different infrastructure.

## Scope boundary

**In scope:**
- A model server process exposing an OpenAI-compatible HTTP API on localhost.
- A LoRA-fine-tuned adapter on top of a small MLX base model, loaded by that server.
- A benchmarking harness measuring server performance, and an optimization pass based on the findings.

**Out of scope (deferred to future specs):**
- Gateway/routing layer in front of the model server.
- Agent/tool-calling orchestration that would consume this model server.
- Any changes to the existing Spark/Flink/Airflow pipeline — this component is a read-only downstream consumer of already-computed fraud alerts/features.

## Architecture

New isolated runtime `llm/`, matching the project's existing pattern of per-subsystem Python environments (root `uv` project, `flink/`, `airflow/`, `datahub/` are each separate).

```
llm/
  pyproject.toml / uv env        # isolated runtime
  models/adapters/                # gitignored LoRA artifacts (build output, not source)
  data/                           # dataset-generation scripts + generated training data (committed)
  serve.py or config               # mlx_lm.server launch config
  benchmark/                       # load-test harness + results
```

**Data flow:** existing pipeline (Flink alert topic / `gold.feat_*` tables / streaming Iceberg tables) → read-only sampling script → dataset generation (`llm/data/`) → LoRA fine-tuning (`mlx_lm.lora`) → adapter loaded by `mlx_lm.server` → OpenAI-compatible HTTP API on localhost → benchmark client exercises that API.

## Model server

**Engine:** `mlx_lm.server` — starts an HTTP server process on the Mac, loads model weights into unified memory, runs generation accelerated by Metal (the GPU), and exposes an OpenAI-compatible API (`POST /v1/chat/completions`, same request/response JSON shape as `api.openai.com`). This is the MLX equivalent of `vllm serve` in the original llm-d guide. "OpenAI-compatible" matters because it lets any existing tooling built against that schema (benchmarking clients, chat UIs, later an agent) point at this server just by changing the base URL — no custom protocol needed. "On localhost" means the server only listens on `127.0.0.1`; nothing outside the machine can reach it unless deliberately exposed.

## Model choice

**Base model: `mlx-community/Qwen2.5-3B-Instruct-4bit`.**

Rationale, evaluated against same-size alternatives (Llama-3.2-3B-Instruct, Phi-3.5-mini-instruct, Gemma-2-2B-it):
- Strongest instruction-following in its size class at release, particularly on structured input/output (JSON/table-style data), which matches this use case — input is structured transaction/feature data, output needs to be grounded prose referencing specific fields.
- Less prone to safety-driven hedging on financial/security-flavored prompts than more heavily safety-tuned alternatives (Gemma, Phi), which matters for "explain why this transaction is suspicious"-style prompts.
- Permissive-enough license (custom Qwen license) for personal/portfolio use.
- Strong `mlx-community` ecosystem coverage — multiple pre-quantized builds (4-bit/8-bit/fp16) available without manual conversion, which is required for the quantization-level comparison in the optimization pass.

**Size:** 3B chosen over a 7-8B alternative (also fits comfortably in 32GB RAM at 4-bit) to keep the fine-tune/benchmark iteration loop fast. This trades some explanation quality for iteration speed — an explicit, revisitable choice, not a hardware constraint.

**Known limitation:** model rankings here reflect training-data knowledge that predates the current date by several months. Before finalizing, worth a quick check of `mlx-community`'s trending models or the Open LLM Leaderboard for anything newer that supersedes this recommendation.

## Custom model: LoRA fine-tuning

**Objective:** adapt output *style and domain focus* for fraud-explanation text — not teach new facts. The base 3B instruct model already has the underlying reasoning ability; LoRA adjusts how it expresses that for this specific task.

**Training data (built new, since nothing like it exists in the repo):**
- `llm/data/sample_alerts.py` — reads a sample of real, already-computed fraud alerts/features from the existing pipeline (Iceberg tables / Postgres serving tables), read-only.
- `llm/data/generate_explanations.py` — template-based generator that produces varied target explanation text per sampled alert (programmatic phrasing/structure variation, not copies of one pattern). Deliberately avoids calling an external LLM API to synthesize training data, keeping this dependency-free and consistent with the rest of the project running locally.
- Output: `train.jsonl` / `valid.jsonl` in `mlx_lm.lora`'s expected chat-formatted pair structure (few hundred examples target — sufficient for style/domain adaptation via LoRA, not a large-corpus undertaking).
- Dataset is committed to git (small plain text, makes the fine-tune step reproducible); the LoRA adapter binary output is not (gitignored build artifact, tens of MB, reproducible from the dataset + training script + documented config).

**Training mechanics:** `mlx_lm.lora` CLI, run locally on the M1 Pro via Metal acceleration (no cloud dependency needed — 32GB RAM and MLX's native LoRA support make this fast enough for a 3B model and a few-hundred-example dataset). Output adapter at `llm/models/adapters/fraud-explainer-v1/`, loaded via `mlx_lm.server --adapter-path ...`.

**Quality check:** qualitative before/after comparison on a held-out set — does fine-tuned output correctly reference input field values, stay concise, avoid generic hedging? This is separate from the performance benchmarking below (quality vs. speed are different concerns).

## Benchmarking & optimization

**Harness:** async Python client (`llm/benchmark/`) hitting `/v1/chat/completions` with fraud-alert-derived prompts (reusing Section "Custom model" prompt samples), measuring at increasing concurrency (1, 2, 4, 8...):
- Time-to-first-token (TTFT)
- Inter-token latency / tokens-per-second
- Requests/sec sustained
- RAM footprint (unified memory on Apple Silicon)

**Open question to resolve empirically first, not assumed:** whether `mlx_lm.server` handles concurrent requests via batching, or processes them serially (MLX's generation loop has not historically had vLLM-style continuous batching). This determines what "optimize" can mean for concurrency scaling and must be measured before drawing conclusions.

**Optimization knobs to compare** (after baseline/bottleneck is established):
- Quantization level: 4-bit vs 8-bit vs fp16 — quality/speed/memory trade-off, using pre-quantized `mlx-community` builds.
- Base model vs. base+LoRA-adapter latency overhead.
- Context length impact on TTFT (short alert vs. longer feature-rich prompt).

**Deliverable:** write-up at `docs/optimization/llm/`, following the existing `docs/optimization/{flink,spark}` pattern: baseline measurement → bottleneck identified → change applied → re-measurement → quantified improvement.

## Testing & error handling

**Testing:** unit tests for dataset-generation logic (deterministic given a seed) and benchmark client metric calculations (percentile math). Model output quality is checked qualitatively (see above), not via automated tests.

**Error handling:** kept minimal and realistic for a local single-user tool — server health-check before a benchmark run starts; the benchmark client records request timeouts/failures as data points rather than crashing the run.

## Explicitly deferred (future specs)

- Gateway/router in front of this model server (was under discussion before this spec, deliberately tabled).
- Agent/tool-calling orchestration layer that would consume this model server as its LLM backend.
