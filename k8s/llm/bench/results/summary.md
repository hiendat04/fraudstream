# Benchmark results

Workload: 1,000 shared prefix tokens + 200 prompt tokens, 200 output tokens.
SLA: TTFT ≤ 8 s and time per output token ≤ 400 ms for 95% of requests.
Goodput: the highest concurrency meeting the SLA, found by guidellm's goodput search.
Each concurrency the search tries runs for 15 minutes, on a laptop (CPU only, inside Docker).

## Limitation

The model runs on a MacBook's CPU inside Docker, with no GPU and about 4–5 usable cores.
The 8 s TTFT limit is already lenient: interactive chat usually targets 1–2 s.
It is kept on purpose rather than raised until the baseline passes. A change counts only if it
moves the numbers toward that limit, and a result that never reaches it shows this workload needs a GPU.

## Results

Each experiment changes one thing in `k8s/llm/models/qwen3-4b.yaml` against the best so far.
Kept = higher goodput; on a tie, more output tokens per second. The report for each row is `<#>-<name>.json`.

| # | Change in `qwen3-4b.yaml` | Goodput (concurrency) | Requests/s | Output tokens/s | TTFT p50 / p95 (s) | Time per output token p50 / p95 (ms) | Kept? |
|---|---|---|---|---|---|---|---|
| 0 | Baseline: `--model /models/Qwen3-4B-Q4_K_M.gguf --threads 8 --parallel 1 --ctx-size 8192`, memory limit 5Gi | none (fails at 1) | 0.02 | 4.0 | 13.0 / 13.5 | 239 / 263 | baseline |
| 1 | `--model /models/Qwen3-4B-Q4_K_M.gguf` → `/models/Qwen3-4B-Q4_0.gguf` | **1** | 0.04 | **7.6** | **5.1 / 5.4** | **125 / 140** | **yes** |
| 2 | `--parallel 1` → `2`, memory limit 5Gi → 6Gi | 1 | 0.03 | 6.9 | 5.4 / 6.5 | 139 / 188 | no |
| 3 | add `--flash-attn on --cache-type-k q8_0 --cache-type-v q8_0` | none (89% at 1) | 0.03 | 6.0 | 6.4 / 9.4 | 136 / 289 | no |
| 4 | `--threads 8` → `6` | 1 | 0.03 | 6.4 | 7.2 / 7.8 | 151 / 165 | no |

**Winner:** Q4_0 weights, 8 threads, 1 slot, 8,192-token context. Only the weights changed from the baseline.

## 0. Baseline

Even one request at a time misses the SLA, so there is no goodput.

- Token speed is fine: about 0.24 s per token, under the 0.4 s limit.
- The first token is the problem: about 13 s, against a limit of 8 s.
- A full request takes about 48 s; 18 finished in 15 minutes.

The shared prefix is cached (1,010 of 1,213 prompt tokens), so the 13 s is spent reading only
the last 200 tokens. The CPU is the limit: the model gets about 4–5 cores inside Docker and no GPU.

## 1. Q4_0 weights

Tried first, because only faster prompt reading can get one request under the TTFT limit.

- TTFT drops from 13.0 s to 5.1 s, and time per token from 239 ms to 125 ms: about 2× faster on both.
- One request at a time now meets the SLA (97% of 34 requests), so goodput is 1.
- Two at a time fails: with one slot the second request waits for the first, so TTFT rises to 30 s.

llama.cpp has a faster arm64 path for Q4_0 weights, which is where the gain comes from.

Quality holds: the tool-call check scores 19/20 (Q4_K_M: 20/20), at the 19/20 bar. The miss is a
definition question answered without searching the handbook first. Kept.

## 2. Two slots

Two requests now run side by side instead of queueing, so at concurrency 2 the SLA is met
for 95% of requests (one slot: 0%). That is right at the 95% bar, and with 37 requests guidellm
cannot be sure it is above it, so goodput stays 1.

- At concurrency 2, TTFT p95 is 18.4 s: the two requests share the same 4–5 cores.
- Total output rises only from 7.6 to 8.2 tokens/s, within run-to-run noise.

Not kept: same goodput, no real gain in speed. Four slots were not tried, since they would split
the same cores four ways.

## 3. 8-bit KV cache

The KV cache holds the prompt the model has already read. Storing it in 8 bits instead of 16 saves
memory, but on this CPU each step now has to convert it back, and that costs time.

- TTFT rises from 5.1 s to 6.4 s; output drops from 7.6 to 6.0 tokens/s.
- 89% of requests meet the SLA at concurrency 1, under the 95% bar, so goodput is lost.

Not kept: memory is not the constraint here, speed is.

## 4. Six threads

With 8 threads the server got only about 4.4 cores, so fewer threads might wait on each other less.
They do not: TTFT rises from 5.1 s to 7.2 s and output drops from 7.6 to 6.4 tokens/s.
The 8 threads use whatever cores the laptop frees up; 6 cannot. Not kept.

## Not tried: prompt cache reuse

`--cache-reuse` helps when a cached prompt reappears shifted inside a new one. Here every request
starts with the same prefix, which llama.cpp already reuses (1,010 of 1,213 tokens from the cache),
so there is nothing left for it to find.
