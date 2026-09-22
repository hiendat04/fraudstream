"""Tests for the drift detection API.

Redis is faked, so these need no server. The reference is a small synthetic one
written to a temporary file, the same shape as the committed one.
"""

import json
import unittest
import unittest.mock
from pathlib import Path
from tempfile import TemporaryDirectory

import fakeredis
import numpy as np
from fastapi.testclient import TestClient

from fraudstream_api.drift_detection.app import create_app
from fraudstream_api.drift_detection.reference import build_reference
from fraudstream_api.drift_detection.settings import Settings

INPUTS = ("amount", "customer_features_available", "txn_count_7d")


def reference_json() -> str:
    rng = np.random.default_rng(0)
    return build_reference(
        {
            "amount": rng.normal(100.0, 20.0, 2000),
            "customer_features_available": np.array([0.0] * 1800 + [1.0] * 200),
            "txn_count_7d": np.where(rng.random(2000) < 0.5, rng.normal(5.0, 1.0, 2000), np.nan),
        },
        model_name="fraud-detection",
        model_version="2",
        data_snapshot_id="3023861480409485916",
    ).to_json()


def rows(count: int, shift: float = 0.0) -> list[dict]:
    """Rows drawn from the reference distribution, with only `amount` moved.

    Sending the same value for every input would shift all of them at once,
    and the report would rank whichever was hardest hit rather than `amount`.
    """

    rng = np.random.default_rng(7)
    amounts = rng.normal(100.0, 20.0, count) + shift
    flags = rng.random(count) < 0.1
    counts = np.where(rng.random(count) < 0.5, rng.normal(5.0, 1.0, count), np.nan)
    return [
        {
            "amount": float(amounts[i]),
            "customer_features_available": bool(flags[i]),
            "txn_count_7d": None if np.isnan(counts[i]) else float(counts[i]),
        }
        for i in range(count)
    ]


class DriftAppTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name) / "reference.json"
        path.write_text(reference_json())
        self.settings = Settings(
            reference_path=str(path), min_observations=50, app_version="test"
        )
        self.server = fakeredis.FakeServer()

    def client(self) -> TestClient:
        redis = fakeredis.FakeAsyncRedis(server=self.server, decode_responses=True)
        return TestClient(create_app(self.settings, redis=redis))

    def test_it_opens_redis_with_decoding_and_short_timeouts(self):
        """Without decoding, every field name comes back as bytes and no count is found."""

        with unittest.mock.patch(
            "fraudstream_api.drift_detection.app.Redis.from_url"
        ) as from_url:
            create_app(self.settings)

        from_url.assert_called_once_with(
            self.settings.redis_url,
            decode_responses=True,
            socket_timeout=1,
            socket_connect_timeout=1,
        )

    def test_observations_are_counted(self):
        with self.client() as client:
            answer = client.post("/v1/observations", json={"observations": rows(10)})

        self.assertEqual(200, answer.status_code)
        self.assertEqual({"accepted": 10}, answer.json())

    def test_an_observation_missing_an_input_is_refused_and_named(self):
        incomplete = [{"amount": 100.0, "customer_features_available": 0.0}]

        with self.client() as client:
            answer = client.post("/v1/observations", json={"observations": incomplete})

        self.assertEqual(422, answer.status_code)
        self.assertIn("txn_count_7d", json.dumps(answer.json()))

    def test_an_observation_with_an_unknown_input_is_refused(self):
        """Rows from a model with other inputs must not be scored against this reference."""

        extra = [{**rows(1)[0], "some_new_feature": 1.0}]

        with self.client() as client:
            answer = client.post("/v1/observations", json={"observations": extra})

        self.assertEqual(422, answer.status_code)
        self.assertIn("some_new_feature", json.dumps(answer.json()))

    def test_too_few_observations_give_no_verdict(self):
        with self.client() as client:
            client.post("/v1/observations", json={"observations": rows(10)})
            report = client.get("/v1/drift").json()

        self.assertEqual("not_enough_data", report["status"])
        self.assertEqual([], report["features"])
        self.assertEqual(10, report["observations"])

    def test_a_shifted_input_is_reported_as_drift(self):
        with self.client() as client:
            client.post("/v1/observations", json={"observations": rows(300, shift=120.0)})
            report = client.get("/v1/drift").json()

        self.assertEqual("drift", report["status"])
        self.assertEqual("amount", report["features"][0]["name"])
        self.assertEqual("drift", report["features"][0]["status"])

    def test_the_report_ranks_inputs_by_psi_and_names_the_model(self):
        with self.client() as client:
            client.post("/v1/observations", json={"observations": rows(300, shift=120.0)})
            report = client.get("/v1/drift").json()

        self.assertEqual("fraud-detection", report["model_name"])
        self.assertEqual("2", report["model_version"])
        self.assertEqual(60, report["window_minutes"])
        scores = [feature["psi"] for feature in report["features"]]
        self.assertEqual(sorted(scores, reverse=True), scores)
        self.assertEqual(sorted(INPUTS), sorted(f["name"] for f in report["features"]))

    def test_liveness_passes_while_redis_is_down(self):
        with self.client() as client:
            self.server.connected = False
            answer = client.get("/livez")

        self.assertEqual(200, answer.status_code)

    def test_readiness_passes_while_redis_answers(self):
        with self.client() as client:
            answer = client.get("/readyz")

        self.assertEqual(200, answer.status_code)
        self.assertEqual({"status": "ready"}, answer.json())

    def test_readiness_fails_while_redis_is_down(self):
        with self.client() as client:
            self.server.connected = False
            answer = client.get("/readyz")

        self.assertEqual(503, answer.status_code)

    def test_every_response_names_the_app_version(self):
        with self.client() as client:
            counted = client.post("/v1/observations", json={"observations": rows(1)})
            refused = client.post("/v1/observations", json={"observations": [{"amount": 1.0}]})
            report = client.get("/v1/drift")

        for answer in (counted, refused, report):
            with self.subTest(status=answer.status_code):
                self.assertEqual("test", answer.headers["x-app-version"])


if __name__ == "__main__":
    unittest.main()
