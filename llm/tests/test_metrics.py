import pytest

from benchmark.metrics import RequestResult, percentile, summarize


def test_percentile_p50_of_sorted_values():
    assert percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == 3.0


def test_percentile_p95_of_larger_set():
    values = [float(i) for i in range(1, 101)]
    assert percentile(values, 95) == 95.0


def test_percentile_raises_on_empty_list():
    with pytest.raises(ValueError):
        percentile([], 50)


def test_summarize_counts_successes_and_failures():
    results = [
        RequestResult(ttft_seconds=0.1, total_seconds=1.0, output_tokens=50, succeeded=True),
        RequestResult(ttft_seconds=0.2, total_seconds=2.0, output_tokens=100, succeeded=True),
        RequestResult(ttft_seconds=0.0, total_seconds=0.0, output_tokens=0, succeeded=False),
    ]

    summary = summarize(results, concurrency=4, wall_clock_seconds=2.0)

    assert summary.request_count == 3
    assert summary.success_count == 2
    assert summary.requests_per_second == 1.0


def test_summarize_computes_mean_tokens_per_second():
    results = [
        RequestResult(ttft_seconds=0.1, total_seconds=1.0, output_tokens=50, succeeded=True),
        RequestResult(ttft_seconds=0.1, total_seconds=2.0, output_tokens=100, succeeded=True),
    ]

    summary = summarize(results, concurrency=1, wall_clock_seconds=3.0)

    assert summary.mean_tokens_per_second == pytest.approx(50.0)
