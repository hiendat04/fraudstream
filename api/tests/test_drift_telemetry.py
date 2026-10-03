"""The drift detection API's own metrics."""

import fakeredis
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from fraudstream_api.drift_detection.app import create_app
from fraudstream_api.drift_detection.settings import Settings

from tests.test_drift_app import rows


def sample(name: str) -> float:
    return REGISTRY.get_sample_value(name) or 0.0


def test_accepted_rows_are_counted(drift_client):
    before = sample("drift_observations_accepted_total")
    drift_client.post("/v1/observations", json={"observations": rows(3)})
    assert sample("drift_observations_accepted_total") == before + 3


def test_a_refused_batch_is_counted_and_adds_no_rows(drift_client):
    accepted = sample("drift_observations_accepted_total")
    refused = sample("drift_batches_rejected_total")
    bad = [{**rows(1)[0], "unexpected": 1.0}]
    assert drift_client.post("/v1/observations", json={"observations": bad}).status_code == 422
    assert sample("drift_batches_rejected_total") == refused + 1
    assert sample("drift_observations_accepted_total") == accepted


def test_the_metrics_port_opens_with_the_app_and_closes_after(drift_reference_path):
    settings = Settings(reference_path=str(drift_reference_path), metrics_port=0)
    app = create_app(settings, redis=fakeredis.FakeAsyncRedis(decode_responses=True))
    with TestClient(app):
        assert app.state.metrics_server.server_port > 0
    assert app.state.metrics_server.socket.fileno() == -1
