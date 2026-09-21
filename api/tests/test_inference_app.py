"""Tests for the inference API: its endpoints and its probes.

Feast and KServe are replaced by fakes, so these need no Redis, no PostgreSQL
and no model.
"""

import unittest
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from fraudstream_api.inference.app import create_app
from fraudstream_api.inference.features import OnlineFeatures
from fraudstream_api.inference.model import ModelRejected, ModelTimeout, ModelUnavailable
from fraudstream_api.inference.settings import Settings

from tests.test_inference_features import FEATURE_NAMES, WITH_HISTORY, training_inputs

PAYMENT = {
    "transaction_id": "demo-000001",
    "customer_id": "cust_00103290",
    "merchant_id": "merch_00023570",
    "event_timestamp": "2026-06-30T20:00:00Z",
    "amount": 36.35,
    "channel": "atm",
    "city": "New York",
}

TTLS = {
    "customer_rolling_features": timedelta(days=31),
    "customer_orders_90d_features": timedelta(days=91),
    "merchant_risk_features": timedelta(days=31),
}

PAID_AT = datetime(2026, 6, 30, 20, 0, tzinfo=UTC)


def online(age=timedelta(days=1)):
    """The store's answer for the payment above: every view present and fresh."""

    return OnlineFeatures(
        values={name: WITH_HISTORY[name] for name in FEATURE_NAMES},
        event_times={view: PAID_AT - age for view in TTLS},
    )


class FakeReader:
    """Stands in for Feast. Holds one answer and counts reads."""

    def __init__(self, answer=None, *, ready=True, error=None):
        self.answer = answer if answer is not None else online()
        self.ready = ready
        self.error = error
        self.reads = 0

    async def start(self):
        pass

    def ttls(self):
        return TTLS

    async def read(self, customer_id, merchant_id):
        self.reads += 1
        if self.error:
            raise self.error
        return self.answer

    async def is_ready(self):
        return self.ready

    async def close(self):
        pass


class FakeModel:
    """Stands in for KServe. Records every row it is asked to score."""

    def __init__(self, probability=0.42, error=None):
        self.probability = probability
        self.error = error
        self.calls = []

    async def score(self, inputs):
        self.calls.append(inputs)
        if self.error:
            raise self.error
        return self.probability

    async def close(self):
        pass


class FakeDrift:
    """Stands in for the drift detection API. Records every row it is sent."""

    def __init__(self):
        self.sent = []

    async def send(self, inputs):
        self.sent.append(inputs)

    async def close(self):
        pass


def build(reader=None, model=None, drift=None):
    settings = Settings(postgres_user="u", postgres_password="p", app_version="test")
    return create_app(
        settings, reader=reader or FakeReader(), model=model or FakeModel(), drift=drift
    )


class InferenceAppTest(unittest.TestCase):
    def test_a_payment_is_scored(self):
        with TestClient(build()) as client:
            answer = client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(200, answer.status_code)
        body = answer.json()
        self.assertEqual("demo-000001", body["transaction_id"])
        self.assertEqual(0.42, body["fraud_probability"])
        self.assertTrue(body["history_found"]["customer_features_available"])

    def test_the_model_receives_exactly_the_51_training_inputs(self):
        model = FakeModel()
        with TestClient(build(model=model)) as client:
            client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(1, len(model.calls))
        sent = model.calls[0]
        self.assertEqual(51, len(sent))
        self.assertEqual(list(training_inputs(WITH_HISTORY)), list(sent))

    def test_an_unknown_customer_still_gets_a_score(self):
        nothing_stored = OnlineFeatures(
            values={name: None for name in FEATURE_NAMES},
            event_times={view: None for view in TTLS},
        )
        model = FakeModel()
        with TestClient(build(reader=FakeReader(nothing_stored), model=model)) as client:
            answer = client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(200, answer.status_code)
        self.assertFalse(answer.json()["history_found"]["customer_features_available"])
        self.assertEqual(51, len(model.calls[0]))

    def test_stale_history_is_not_sent_to_the_model(self):
        model = FakeModel()
        stale = FakeReader(online(age=timedelta(days=40)))
        with TestClient(build(reader=stale, model=model)) as client:
            client.post("/v1/predict", json=PAYMENT)

        sent = model.calls[0]
        self.assertIsNone(sent["txn_count_7d"])
        self.assertFalse(sent["customer_features_available"])

    def test_a_bad_request_is_refused_before_the_store_is_read(self):
        reader = FakeReader()
        with TestClient(build(reader=reader)) as client:
            answer = client.post("/v1/predict", json={**PAYMENT, "channel": "fax"})

        self.assertEqual(422, answer.status_code)
        self.assertEqual(0, reader.reads)

    def test_model_failures_become_502_503_504(self):
        for error, expected in (
            (ModelTimeout("slow"), 504),
            (ModelUnavailable("no route"), 503),
            (ModelRejected("400: nope"), 502),
        ):
            with self.subTest(error=type(error).__name__):
                with TestClient(build(model=FakeModel(error=error))) as client:
                    answer = client.post("/v1/predict", json=PAYMENT)
                self.assertEqual(expected, answer.status_code)

    def test_an_online_store_outage_becomes_503(self):
        broken = FakeReader(error=RedisConnectionError("redis is down"))
        with TestClient(build(reader=broken)) as client:
            answer = client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(503, answer.status_code)

    def test_liveness_passes_while_the_store_is_down(self):
        """Restarting this pod cannot fix Redis, so liveness must not fail with it."""

        with TestClient(build(reader=FakeReader(ready=False))) as client:
            answer = client.get("/livez")

        self.assertEqual(200, answer.status_code)

    def test_readiness_fails_while_the_store_is_down(self):
        with TestClient(build(reader=FakeReader(ready=False))) as client:
            answer = client.get("/readyz")

        self.assertEqual(503, answer.status_code)

    def test_readiness_never_calls_the_model(self):
        """Probe traffic would keep the model awake and stop it scaling to zero."""

        model = FakeModel()
        with TestClient(build(model=model)) as client:
            for _ in range(10):
                client.get("/readyz")

        self.assertEqual([], model.calls)

    def test_every_response_names_the_app_version(self):
        with TestClient(build(reader=FakeReader(ready=False))) as client:
            scored = client.post("/v1/predict", json=PAYMENT)
            refused = client.post("/v1/predict", json={**PAYMENT, "amount": -1})
            unready = client.get("/readyz")

        for answer in (scored, refused, unready):
            with self.subTest(status=answer.status_code):
                self.assertEqual("test", answer.headers["x-app-version"])


class DriftFeedWiringTest(unittest.TestCase):
    def test_a_prediction_sends_the_exact_model_inputs_to_the_drift_detector(self):
        model, drift = FakeModel(), FakeDrift()

        with TestClient(build(model=model, drift=drift)) as client:
            client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(1, len(drift.sent))
        self.assertEqual(model.calls[0], drift.sent[0])

    def test_a_failed_prediction_sends_nothing(self):
        """The window must hold only what the model actually scored."""

        drift = FakeDrift()
        broken = FakeModel(error=ModelUnavailable("no route"))

        with TestClient(build(model=broken, drift=drift)) as client:
            answer = client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(503, answer.status_code)
        self.assertEqual([], drift.sent)

    def test_a_refused_payment_sends_nothing(self):
        drift = FakeDrift()

        with TestClient(build(drift=drift)) as client:
            client.post("/v1/predict", json={**PAYMENT, "channel": "fax"})

        self.assertEqual([], drift.sent)

    def test_without_a_drift_url_nothing_is_sent(self):
        """The inference API runs on its own when no drift detector is configured."""

        with TestClient(build()) as client:
            answer = client.post("/v1/predict", json=PAYMENT)

        self.assertEqual(200, answer.status_code)


if __name__ == "__main__":
    unittest.main()
