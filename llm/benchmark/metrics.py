from dataclasses import dataclass


@dataclass(frozen=True)
class RequestResult:
    ttft_seconds: float
    total_seconds: float
    output_tokens: int
    succeeded: bool


@dataclass(frozen=True)
class BenchmarkSummary:
    concurrency: int
    request_count: int
    success_count: int
    p50_ttft_seconds: float
    p95_ttft_seconds: float
    mean_tokens_per_second: float
    requests_per_second: float


def percentile(values: list[float], pct: float) -> float:
    if not values:
        raise ValueError("cannot compute percentile of empty list")
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[index]


def summarize(
    results: list[RequestResult], concurrency: int, wall_clock_seconds: float
) -> BenchmarkSummary:
    successes = [r for r in results if r.succeeded]
    ttfts = [r.ttft_seconds for r in successes]
    tokens_per_second = [
        r.output_tokens / r.total_seconds for r in successes if r.total_seconds > 0
    ]

    return BenchmarkSummary(
        concurrency=concurrency,
        request_count=len(results),
        success_count=len(successes),
        p50_ttft_seconds=percentile(ttfts, 50) if ttfts else float("nan"),
        p95_ttft_seconds=percentile(ttfts, 95) if ttfts else float("nan"),
        mean_tokens_per_second=(
            sum(tokens_per_second) / len(tokens_per_second) if tokens_per_second else 0.0
        ),
        requests_per_second=(
            len(successes) / wall_clock_seconds if wall_clock_seconds > 0 else 0.0
        ),
    )
