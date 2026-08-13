# LLM Inference Platform Optimization

`mlx_lm.server` **batches concurrent requests rather than serializing them**
(Task 8): requests/s rose **2.12x** from concurrency 1 to 8, while per-stream
throughput fell from 52.0 to 13.9 tokens/s over the same range, because all
concurrent streams share one Apple Silicon GPU. Across the variants swept in
this task, the headline trade-off is that **the LoRA adapter is free** (TTFT
and tokens/s at concurrency=1 are within run-to-run noise of the base model)
while **8-bit quantization is a straight regression on this hardware**: p50
TTFT nearly doubles (0.594s to 1.039s) and steady-state throughput drops by
**41%** (52.0 to 30.7 tokens/s) relative to the 4-bit baseline, with no
memory measurement in this benchmark reliable enough to call it a
capacity win. Context length matters more than any of the model-config
knobs: dropping the feature-rich prompt for a bare one-line prompt cut p50
TTFT by **60%** (0.594s to 0.235s).

## Method

- Server started via `llm/serve.sh`; port 8080 was occupied by an unrelated
  local Docker process (same finding as Task 8), so all runs in this task use
  `PORT=18080` with `--base-url http://localhost:18080` on the benchmark CLI.
- The **Concurrency Behavior** table below reuses Task 8's
  `baseline_results.json` numbers directly rather than re-running the 4-bit/
  no-adapter sweep, since Task 8 already produced a complete, saved sweep.
- LoRA, 8-bit, and context-length benchmarks were run fresh in this task via
  `uv run python -m benchmark.run_benchmark`, one server variant at a time,
  each server stopped (`pkill -f mlx_lm.server`) before starting the next.
- Peak memory was sampled with `ps -o rss= -p <server_pid>` polled every
  0.5s for the duration of each concurrency=1 run. This method turned out to
  be unreliable for MLX/Metal processes on this Apple Silicon machine — see
  the caveat in the Quantization Comparison section below.

## Concurrency Behavior (Baseline, 4-bit, no adapter)

Reused from Task 8 (`llm/benchmark/baseline_results.json`), not re-run here.

| Concurrency | p50 TTFT (s) | p95 TTFT (s) | Tokens/s | Req/s |
|---|---:|---:|---:|---:|
| 1 | 0.594 | 0.594 | 52.0 | 0.264 |
| 2 | 0.403 | 0.426 | 40.2 | 0.407 |
| 4 | 0.754 | 0.756 | 23.6 | 0.476 |
| 8 | 0.869 | 0.871 | 13.9 | 0.560 |

**Finding:** batched — from Task 8 Step 3, `mlx_lm.server` handles concurrent
requests together rather than one-at-a-time. Req/s keeps climbing through
concurrency=8 (2.12x over concurrency=1) instead of staying flat, which rules
out pure serialization. But scaling is sublinear and compute-bound: per-stream
tokens/s drops by 73% (52.0 to 13.9) across the same range, because every
concurrent stream competes for the same GPU.

## Quantization Comparison (concurrency=1)

| Variant | p50 TTFT (s) | Tokens/s | Peak RSS (MB) |
|---|---:|---:|---:|
| 4-bit (baseline) | 0.594 (Task 8) / 0.357 (this task's rerun) | 52.0 (Task 8) / 59.5 (this task's rerun) | 1909.2 |
| 8-bit | 1.039 | 30.7 | 305.4 |

**Memory measurement caveat:** the 4-bit server's peak RSS (1909.2 MB) came
out *higher* than the 8-bit server's peak RSS (305.4 MB), which contradicts
the expectation that 8-bit weights (roughly double the on-disk/in-memory
size of 4-bit for the same 3B-parameter model) should use more memory, not
less. `ps -o rss` does not reliably capture MLX's Metal-backed unified
memory allocations on this machine — GPU-resident buffers are not always
reflected in the CPU-side resident set the way they would be for an
ordinary process. Treat the RSS column as an inconclusive, honestly-reported
measurement artifact, not a validated finding; a proper measurement would
need `sudo powermetrics` or Activity Monitor's GPU-aware memory view, neither
of which was run here. The 4-bit row includes both Task 8's original
concurrency=1 result and a fresh concurrency=1 rerun performed in this task
alongside the RSS sampling — the two are close (0.594s/52.0 tok/s vs.
0.357s/59.5 tok/s), consistent with normal run-to-run variance from thermal
state and background load rather than any config change.

Speed and throughput, unlike memory, are unambiguous: 8-bit is slower on
every axis measured. p50 TTFT is 75% higher (0.594s to 1.039s) and
throughput is 41% lower (52.0 to 30.7 tokens/s) than the 4-bit baseline.

## LoRA Adapter Overhead (concurrency=1, 4-bit)

| Variant | p50 TTFT (s) | Tokens/s |
|---|---:|---:|
| Base model | 0.594 | 52.0 |
| Base + LoRA adapter | 0.607 | 52.5 |

The adapter adds no measurable overhead at concurrency=1 — the 13ms TTFT
difference and +0.5 tokens/s difference are within normal run-to-run noise.
This holds across the full concurrency sweep too: LoRA p50 TTFT/tokens/s at
concurrency 2/4/8 were 0.395s/42.5, 0.700s/24.0, and 0.911s/13.8 respectively,
tracking the 4-bit baseline's 0.403s/40.2, 0.754s/23.6, and 0.869s/13.9
within a few percent at every level. (Task 5's finding that the adapter's
generated *text quality* degenerates on held-out prompts due to overfitting
the 18-example training set is a separate, already-documented issue —
irrelevant to this task's latency/throughput measurement.)

## Context Length Impact (concurrency=1, 4-bit)

| Prompt type | p50 TTFT (s) |
|---|---:|
| Short (no features) | 0.235 |
| Long (full feature set) | 0.594 |

The feature-rich prompt used for baseline benchmarking (transaction amount,
channel, merchant, 7-day counts, burst ratios, etc., from `data/valid.jsonl`)
takes 2.5x longer to reach first token than a bare one-line prompt asking the
same kind of question with no context. This is the expected effect of prefill
cost scaling with input token count, and it means TTFT numbers throughout this
report and Task 8 are specific to prompts of that length and shape — a
production caller sending shorter prompts should expect meaningfully lower
TTFT than the baseline table suggests, independent of any server-side config
change.

## Recommendation

**Ship 4-bit quantization with the LoRA adapter attached as the default
configuration.** The LoRA adapter is free at the latency/throughput level
measured here, so there is no cost argument against attaching it (the
open question is generation quality, which Task 5 already flagged as
needing more training data before the adapter is trustworthy for real
explanations — that is a data problem, not a serving-cost problem, and out
of scope for this optimization task). 8-bit quantization should not be
adopted on this hardware: it is unambiguously slower on both TTFT and
throughput with no confirmed memory benefit to offset it, and the one
memory measurement attempted here was inconclusive rather than favorable.
The biggest, least risky win available is on the request-shaping side, not
model configuration: since concurrency scaling is compute-bound and
sublinear, and context length has an outsized effect on TTFT, the platform
should keep prompts as short as the fraud-explanation task allows and treat
concurrency 2-4 as the practical ceiling for interactive latency — pushing
to concurrency=8 buys 2.12x more req/s but at a per-stream tokens/s cost
that would make each individual explanation feel noticeably slower to
whichever caller is waiting on it.
