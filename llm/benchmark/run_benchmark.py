import argparse
import asyncio
import json
import sys

import httpx

from benchmark.client import run_concurrency_level
from benchmark.metrics import summarize


def load_prompts(path: str) -> list[str]:
    prompts = []
    with open(path) as f:
        for line in f:
            example = json.loads(line)
            prompts.append(example["messages"][0]["content"])
    return prompts


def check_server_is_up(base_url: str) -> None:
    try:
        response = httpx.get(f"{base_url}/v1/models", timeout=5.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        print(
            f"Model server at {base_url} is not responding ({exc}). "
            "Start it first with ./serve.sh before running the benchmark.",
            file=sys.stderr,
        )
        sys.exit(1)


async def sweep(base_url: str, prompts: list[str], concurrency_levels: list[int]) -> list[dict]:
    summaries = []
    for concurrency in concurrency_levels:
        batch = (prompts * ((concurrency // len(prompts)) + 1))[:concurrency]
        results, wall_clock = await run_concurrency_level(base_url, batch, concurrency)
        summary = summarize(results, concurrency, wall_clock)
        summaries.append(summary.__dict__)
        print(
            f"concurrency={concurrency} "
            f"p50_ttft={summary.p50_ttft_seconds:.3f}s "
            f"p95_ttft={summary.p95_ttft_seconds:.3f}s "
            f"tokens/s={summary.mean_tokens_per_second:.1f} "
            f"req/s={summary.requests_per_second:.2f}"
        )
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--prompts-path", default="data/valid.jsonl")
    parser.add_argument("--concurrency-levels", default="1,2,4,8")
    parser.add_argument("--out", default="benchmark/results.json")
    args = parser.parse_args()

    check_server_is_up(args.base_url)

    prompts = load_prompts(args.prompts_path)
    levels = [int(x) for x in args.concurrency_levels.split(",")]

    summaries = asyncio.run(sweep(args.base_url, prompts, levels))

    with open(args.out, "w") as f:
        json.dump(summaries, f, indent=2)


if __name__ == "__main__":
    main()
