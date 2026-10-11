"""Read one window of the canary's and the served version's numbers from Prometheus."""

import math

import httpx

from fraudstream_monitoring.rollout_analysis import VersionStats, WindowMetrics


def by_version(client: httpx.Client, expr: str) -> dict[str, float]:
    """One value per model_version. NaN, which Prometheus gives a version with no traffic, is left out."""

    answer = client.get("/api/v1/query", params={"query": expr})
    answer.raise_for_status()
    values = {}
    for row in answer.json()["data"]["result"]:
        value = float(row["value"][1])
        if not math.isnan(value):
            values[row["metric"].get("model_version", "")] = value
    return values


def window_metrics(client: httpx.Client, *, candidate: str, window_seconds: int) -> WindowMetrics:
    """The candidate against the version that answered most of the rest of the traffic."""

    w = f"{int(window_seconds)}s"
    answers = by_version(client, f"sum by (model_version) (increase(fraud_score_count[{w}]))")
    p95 = by_version(
        client,
        "histogram_quantile(0.95, sum by (le, model_version) "
        f'(rate(fraud_model_request_duration_seconds_bucket{{model_version!="unknown"}}[{w}])))',
    )
    median = by_version(client, f"histogram_quantile(0.5, sum by (le, model_version) (rate(fraud_score_bucket[{w}])))")
    flagged = by_version(client, f"sum by (model_version) (increase(fraud_flagged_total[{w}]))")
    thresholds = by_version(client, "max by (model_version) (fraud_model_threshold)")
    failures = sum(by_version(
        client,
        f'sum(increase(http_requests_total{{service="inference-api",route="/v1/predict",status=~"5.."}}[{w}]))',
    ).values())

    others = [v for v in answers if v not in (candidate, "unknown")]
    baseline = max(others, key=answers.get, default="")

    def stats(version: str) -> VersionStats:
        count = answers.get(version, 0.0)
        # A version without a threshold flags nothing; its flag rate is unknown, not 0.
        flag_rate = flagged.get(version, 0.0) / count if version in thresholds and count else None
        return VersionStats(count, p95.get(version), median.get(version), flag_rate)

    return WindowMetrics(candidate=stats(candidate), baseline=stats(baseline), failures=failures)
