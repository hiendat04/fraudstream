# Benchmark results

Workload: 1,000 shared prefix tokens + 200 prompt tokens, 200 output tokens.
SLA: TTFT ≤ 8 s and time per output token ≤ 400 ms for 95% of requests.
Goodput: the highest concurrency meeting the SLA, found by guidellm's goodput search.
Each run lasts 15 minutes on a laptop (CPU only, inside Docker).

## Limitation

The model runs on a MacBook's CPU inside Docker, with no GPU and about 4–5 usable cores.
The 8 s TTFT limit is already lenient: interactive chat usually targets 1–2 s.
It is kept on purpose rather than raised until the baseline passes. A change counts only if it
moves the numbers toward that limit, and a result that never reaches it shows this workload needs a GPU.

| # | Change | Goodput (concurrency) | Requests/s | Output tokens/s | TTFT p50 / p95 (s) | Time per output token p50 / p95 (ms) | Kept? |
|---|---|---|---|---|---|---|---|
| 0 | Baseline: Q4_K_M, 8 threads, 1 slot | none (fails at 1) | 0.02 | 4.0 | 13.0 / 13.5 | 239 / 263 | baseline |

## 0. Baseline

Even one request at a time misses the SLA, so there is no goodput.

- Token speed is fine: about 0.24 s per token, under the 0.4 s limit.
- The first token is the problem: about 13 s, against a limit of 8 s.
- A full request takes about 48 s; 18 finished in 15 minutes.

The shared prefix is cached (1,010 of 1,213 prompt tokens), so the 13 s is spent reading only
the last 200 tokens. The CPU is the limit: the model gets about 4–5 cores inside Docker and no GPU.

