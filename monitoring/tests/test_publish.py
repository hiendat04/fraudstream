import httpx
import pytest
from prometheus_client.exposition import CONTENT_TYPE_LATEST

from fraudstream_monitoring.gateway import gateway_client
from fraudstream_monitoring.publish import drift_metrics, group_path, push

from tests.test_summary import summary


class Recorder:
    def __init__(self, status=200):
        self.status = status
        self.requests = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status)


@pytest.fixture(autouse=True)
def gateway_env(monkeypatch):
    monkeypatch.setenv("GATEWAY_USER", "fraudstream")
    monkeypatch.setenv("GATEWAY_PASSWORD", "secret")
    monkeypatch.delenv("GATEWAY_CA", raising=False)


def pushed(summary_value, status=200) -> tuple[httpx.Request, str]:
    recorder = Recorder(status)
    with gateway_client("https://pushgateway.test", transport=httpx.MockTransport(recorder)) as client:
        push(client, drift_metrics(summary_value), job="fraud_drift", grouping={"window_end": summary_value.window_end})
    request = recorder.requests[0]
    return request, request.content.decode()


def test_replaces_the_windows_group():
    request, _ = pushed(summary())
    assert request.method == "PUT"
    assert request.url.path == "/metrics/job/fraud_drift/window_end/2026-06-30"
    assert request.headers["content-type"] == CONTENT_TYPE_LATEST


def test_goes_through_the_gateway_with_the_password():
    request, _ = pushed(summary())
    assert request.headers["authorization"].startswith("Basic ")


def test_carries_every_inputs_psi():
    body = pushed(summary("drift", [
        {"name": "amount", "psi": 0.61, "status": "drift"},
        {"name": "event_hour", "psi": 0.12, "status": "warning"},
    ]))[1]
    assert 'fraud_drift_feature_psi{feature="amount",model_name="fraud-detection",model_version="2"} 0.61' in body
    assert 'fraud_drift_features_drifted{model_name="fraud-detection",model_version="2"} 1.0' in body
    assert 'fraud_drift_features_warning{model_name="fraud-detection",model_version="2"} 1.0' in body
    assert 'fraud_drift_status{model_name="fraud-detection",model_version="2"} 2.0' in body


def test_not_enough_data_is_still_published():
    body = pushed(summary("not_enough_data"))[1]
    assert 'fraud_drift_status{model_name="fraud-detection",model_version="2"} -1.0' in body
    assert 'fraud_drift_rows{model_name="fraud-detection",model_version="2"} 20000.0' in body
    assert "fraud_drift_feature_psi{" not in body


def test_says_when_the_window_ends_and_how_much_history_there_was():
    body = pushed(summary())[1]
    assert 'fraud_drift_window_end_timestamp_seconds{model_name="fraud-detection",model_version="2"} 1.7827776e+09' in body
    assert 'fraud_drift_history_available_ratio{flag="customer_features_available",model_name="fraud-detection",model_version="2"} 0.4' in body


def test_a_refused_push_fails_loudly():
    with pytest.raises(httpx.HTTPStatusError):
        pushed(summary(), status=500)


def test_group_values_are_escaped():
    assert group_path("fraud drift", {"window end": "a b"}) == "/metrics/job/fraud%20drift/window%20end/a%20b"
