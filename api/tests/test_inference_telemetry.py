"""The inference API's own metrics: the model, the online store and the scores it gives."""

import logging
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from redis.exceptions import ConnectionError as RedisConnectionError

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from fraudstream_api.inference.app import create_app
from fraudstream_api.inference.model import (
    UNKNOWN_VERSION,
    ModelAnswer,
    ModelRejected,
    ModelTimeout,
    ModelUnavailable,
)
from fraudstream_api.inference.telemetry import scored
from fraudstream_api.inference.settings import Settings

from tests.test_inference_app import PAYMENT, FakeModel, FakeReader, build, online


def sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def predict(app) -> int:
    with TestClient(app) as client:
        return client.post("/v1/predict", json=PAYMENT).status_code


@pytest.mark.parametrize(
    "error, reason",
    [
        (ModelTimeout("30 s"), "timeout"),
        (ModelUnavailable("connection refused"), "unavailable"),
        (ModelRejected("400: bad input"), "rejected"),
    ],
)
def test_a_failed_model_call_is_counted_by_reason(error, reason):
    before = sample("fraud_model_failures_total", {"reason": reason})
    predict(build(model=FakeModel(error=error)))
    assert sample("fraud_model_failures_total", {"reason": reason}) == before + 1


def test_a_failed_model_call_is_logged(caplog):
    with caplog.at_level(logging.WARNING, logger="fraudstream_api.inference.telemetry"):
        predict(build(model=FakeModel(error=ModelTimeout("30 s"))))
    assert "timeout" in caplog.text


def test_every_model_call_is_timed_failures_included():
    answered = {"model_version": "2"}
    failed = {"model_version": UNKNOWN_VERSION}
    before = (
        sample("fraud_model_request_duration_seconds_count", answered),
        sample("fraud_model_request_duration_seconds_count", failed),
    )
    predict(build())
    predict(build(model=FakeModel(error=ModelTimeout("30 s"))))
    assert sample("fraud_model_request_duration_seconds_count", answered) == before[0] + 1
    assert sample("fraud_model_request_duration_seconds_count", failed) == before[1] + 1


def test_an_unreachable_online_store_is_counted():
    before = sample("fraud_online_store_failures_total")
    assert predict(build(reader=FakeReader(error=RedisConnectionError("down")))) == 503
    assert sample("fraud_online_store_failures_total") == before + 1


def test_the_score_lands_in_its_bucket_under_its_version():
    below = sample("fraud_score_bucket", {"le": "0.4", "model_version": "4"})
    above = sample("fraud_score_bucket", {"le": "0.45", "model_version": "4"})
    predict(build(model=FakeModel(probability=0.42, version="4")))
    assert sample("fraud_score_bucket", {"le": "0.4", "model_version": "4"}) == below
    assert sample("fraud_score_bucket", {"le": "0.45", "model_version": "4"}) == above + 1


def test_a_failed_prediction_gives_no_score():
    before = sample("fraud_score_count", {"model_version": UNKNOWN_VERSION})
    predict(build(model=FakeModel(error=ModelUnavailable("down"))))
    assert sample("fraud_score_count", {"model_version": UNKNOWN_VERSION}) == before


@pytest.mark.parametrize("probability, flagged", [(0.71, 1), (0.7099, 0), (0.9, 1)])
def test_the_flag_rule_at_the_threshold(probability, flagged):
    labels = {"model_version": "7"}
    before = sample("fraud_flagged_total", labels)
    predict(build(model=FakeModel(probability=probability, version="7", threshold=0.71)))
    assert sample("fraud_flagged_total", labels) == before + flagged


def test_no_threshold_flags_nothing():
    labels = {"model_version": "8"}
    before = sample("fraud_flagged_total", labels)
    predict(build(model=FakeModel(probability=0.99, version="8", threshold=None)))
    assert sample("fraud_flagged_total", labels) == before
    assert REGISTRY.get_sample_value("fraud_model_threshold", labels) is None


def test_the_threshold_of_each_version_is_published():
    predict(build(model=FakeModel(version="9", threshold=0.33)))
    assert sample("fraud_model_threshold", {"model_version": "9"}) == 0.33


def test_the_answering_version_is_put_on_the_current_span():
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    with provider.get_tracer("test").start_as_current_span("request"):
        scored(ModelAnswer(0.3, "4", 0.5), 0.01, {})
    assert spans.get_finished_spans()[0].attributes["model.version"] == "4"


def test_missing_history_is_counted_per_flag():
    flag = {"flag": "customer_features_available"}
    before = sample("fraud_history_missing_total", flag)
    predict(build(reader=FakeReader(answer=online(age=timedelta(days=100)))))
    assert sample("fraud_history_missing_total", flag) == before + 1


def test_present_history_is_not_counted():
    flag = {"flag": "merchant_features_available"}
    before = sample("fraud_history_missing_total", flag)
    predict(build())
    assert sample("fraud_history_missing_total", flag) == before


def test_the_metrics_port_opens_with_the_app_and_closes_after():
    settings = Settings(postgres_user="u", postgres_password="p", app_version="test", metrics_port=0)
    app = create_app(settings, reader=FakeReader(), model=FakeModel())
    with TestClient(app):
        assert app.state.metrics_server.server_port > 0
    assert app.state.metrics_server.socket.fileno() == -1


def test_no_metrics_port_unless_asked():
    app = build()
    with TestClient(app):
        assert app.state.metrics_server is None
