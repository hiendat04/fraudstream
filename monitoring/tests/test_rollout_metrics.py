import math

import httpx

from fraudstream_monitoring.rollout_metrics import by_version, window_metrics


def prometheus(answers: dict[str, list]):
    """A fake Prometheus: the first query whose text contains a key gets that key's rows."""

    def handler(request: httpx.Request) -> httpx.Response:
        query = request.url.params["query"]
        for key, rows in answers.items():
            if key in query:
                result = [{"metric": {"model_version": v} if v else {}, "value": [0, str(x)]} for v, x in rows]
                return httpx.Response(200, json={"status": "success", "data": {"result": result}})
        return httpx.Response(200, json={"status": "success", "data": {"result": []}})

    return httpx.Client(base_url="http://prometheus.test", transport=httpx.MockTransport(handler))


def test_values_are_read_by_version():
    client = prometheus({"fraud_score_count": [("2", 800), ("4", 200)]})
    assert by_version(client, "sum by (model_version) (increase(fraud_score_count[10m]))") == {"2": 800.0, "4": 200.0}


def test_nan_from_prometheus_is_no_data():
    client = prometheus({"histogram_quantile": [("4", math.nan), ("2", 0.012)]})
    assert by_version(client, "histogram_quantile(0.95, x)") == {"2": 0.012}


def test_a_window_compares_the_candidate_with_the_busiest_other_version():
    client = prometheus({
        "increase(fraud_score_count": [("2", 800), ("4", 200), ("unknown", 5)],
        "fraud_model_request_duration_seconds_bucket": [("2", 0.010), ("4", 0.012)],
        "fraud_score_bucket": [("2", 0.30), ("4", 0.31)],
        "fraud_flagged_total": [("2", 40), ("4", 12)],
        "fraud_model_threshold": [("2", 0.619), ("4", 0.626)],
        "http_requests_total": [("", 3)],
    })
    metrics = window_metrics(client, candidate="4", window_seconds=600)
    assert metrics.candidate.answers == 200
    assert metrics.baseline.answers == 800
    assert metrics.candidate.flag_rate == 12 / 200
    assert metrics.baseline.flag_rate == 40 / 800
    assert metrics.failures == 3


def test_a_version_without_a_threshold_has_no_flag_rate():
    client = prometheus({"increase(fraud_score_count": [("2", 800), ("4", 200)]})
    metrics = window_metrics(client, candidate="4", window_seconds=600)
    assert metrics.candidate.flag_rate is None
