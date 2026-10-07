"""Tests for the predictor that serves the fraud model.

These register a small model in a local SQLite MLflow store, the same way the
pipeline registers the real one, and load it back through the predictor. No
server and no cluster are needed.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from fraudstream_serving.predictor import FraudPredictor, registry_reference

COLUMNS = ["amount", "customer_txn_count_30d", "event_hour"]


def _register_model(tracking_uri: str, threshold: float | None = 0.7) -> str:
    """Save a small model to MLflow and return a URI the predictor can load."""

    import mlflow

    rng = np.random.default_rng(0)
    features = pd.DataFrame(rng.random((300, 3)), columns=COLUMNS)
    labels = (features["amount"] > 0.5).astype(int)

    from xgboost import XGBClassifier

    model = XGBClassifier(max_depth=3, n_estimators=10).fit(features, labels)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("serving-test")
    with mlflow.start_run():
        if threshold is not None:
            mlflow.log_metric("threshold", threshold)
        mlflow.xgboost.log_model(model, name="model", registered_model_name="test-fraud")
    return "models:/test-fraud/1"


class PredictorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.tracking_uri = f"sqlite:///{Path(cls._tmp.name) / 'mlflow.db'}"
        cls.model_uri = _register_model(cls.tracking_uri)

        cls.predictor = FraudPredictor(
            name="test-fraud", model_uri=cls.model_uri, tracking_uri=cls.tracking_uri
        )
        cls.predictor.load()

        rng = np.random.default_rng(7)
        cls.rows = pd.DataFrame(rng.random((5, 3)), columns=COLUMNS)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def scores(self, instances: list[dict]) -> list[float]:
        return self.predictor.predict({"instances": instances})["predictions"]

    # ------------------------------------------------------------------

    def test_it_returns_probabilities_not_labels(self):
        """A probability says how likely fraud is. A label only says yes or no.

        The plain MLflow loader returns labels, so this checks the right one is
        being used.
        """

        scores = self.scores(self.rows.to_dict("records"))

        self.assertEqual(5, len(scores))
        for score in scores:
            self.assertGreater(score, 0.0)
            self.assertLess(score, 1.0)

    def test_it_scores_the_same_as_the_model_on_its_own(self):
        import mlflow

        mlflow.set_tracking_uri(self.tracking_uri)
        model = mlflow.xgboost.load_model(self.model_uri)
        expected = model.predict_proba(self.rows)[:, 1]

        np.testing.assert_allclose(self.scores(self.rows.to_dict("records")), expected, rtol=1e-6)

    def test_the_order_of_the_fields_does_not_change_the_score(self):
        """Fields are matched by name, so the sender can send them in any order.

        Without this, a caller that sends columns in a different order gets
        somebody else's score back and no error.
        """

        straight = self.scores(self.rows.to_dict("records"))
        reversed_fields = [
            {key: row[key] for key in reversed(COLUMNS)} for row in self.rows.to_dict("records")
        ]

        np.testing.assert_allclose(self.scores(reversed_fields), straight, rtol=1e-9)

    def test_a_missing_field_is_refused_and_named(self):
        """The answer must say the request was wrong, not that the server broke."""

        from kserve.errors import InvalidInput

        incomplete = [{"amount": 0.4, "event_hour": 13.0}]

        with self.assertRaises(InvalidInput) as caught:
            self.scores(incomplete)
        self.assertIn("customer_txn_count_30d", str(caught.exception))

    def test_without_a_tracking_uri_it_uses_the_one_mlflow_already_has(self):
        import mlflow

        mlflow.set_tracking_uri(self.tracking_uri)
        predictor = FraudPredictor(name="test-fraud", model_uri=self.model_uri)

        self.assertTrue(predictor.load())
        self.assertEqual(COLUMNS, predictor.feature_names)

    def test_an_extra_field_is_ignored(self):
        with_extra = [{**row, "not_a_feature": 99.0} for row in self.rows.to_dict("records")]

        np.testing.assert_allclose(
            self.scores(with_extra), self.scores(self.rows.to_dict("records")), rtol=1e-9
        )



class RegistryReferenceTest(unittest.TestCase):
    def test_a_numbered_version(self):
        self.assertEqual(("fraud-detection", "4"), registry_reference("models:/fraud-detection/4"))

    def test_an_alias(self):
        self.assertEqual(
            ("fraud-detection", "@champion"), registry_reference("models:/fraud-detection@champion")
        )

    def test_anything_else_has_no_registry_version(self):
        self.assertIsNone(registry_reference("runs:/abc123/model"))
        self.assertIsNone(registry_reference("s3://bucket/model"))

    def test_a_logged_model_id_has_no_registry_version(self):
        """MLflow 3 also writes models:/m-<id> for a logged model, with no version in it."""

        self.assertIsNone(registry_reference("models:/m-1a2b3c"))


class VersionAndThresholdTest(unittest.TestCase):
    """Each answer names the version and threshold, so a traffic split can be told apart."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.tracking_uri = f"sqlite:///{Path(self.tmp.name) / 'mlflow.db'}"

    def tearDown(self):
        self.tmp.cleanup()

    def loaded(self, model_uri: str) -> FraudPredictor:
        predictor = FraudPredictor("fraud-detection", model_uri, self.tracking_uri)
        predictor.load()
        return predictor

    def test_it_knows_its_version_and_threshold(self):
        predictor = self.loaded(_register_model(self.tracking_uri, threshold=0.7))
        self.assertEqual("1", predictor.version)
        self.assertEqual(0.7, predictor.threshold)

    def test_every_answer_names_the_version_and_threshold(self):
        predictor = self.loaded(_register_model(self.tracking_uri, threshold=0.7))
        row = {"amount": 0.9, "customer_txn_count_30d": 0.1, "event_hour": 0.5}
        answer = predictor.predict({"instances": [row]})
        self.assertEqual("1", answer["model_version"])
        self.assertEqual(0.7, answer["threshold"])
        self.assertEqual(1, len(answer["predictions"]))

    def test_an_alias_is_resolved_to_its_number(self):
        import mlflow

        _register_model(self.tracking_uri)
        mlflow.MlflowClient(self.tracking_uri).set_registered_model_alias("test-fraud", "champion", "1")
        predictor = self.loaded("models:/test-fraud@champion")
        self.assertEqual("1", predictor.version)

    def test_a_model_outside_the_registry_is_unknown(self):
        import mlflow

        _register_model(self.tracking_uri)
        run_id = mlflow.MlflowClient(self.tracking_uri).get_model_version("test-fraud", "1").run_id
        predictor = self.loaded(f"runs:/{run_id}/model")
        self.assertEqual("unknown", predictor.version)
        self.assertIsNone(predictor.threshold)

    def test_a_run_without_a_threshold_answers_none(self):
        predictor = self.loaded(_register_model(self.tracking_uri, threshold=None))
        self.assertIsNone(predictor.threshold)


if __name__ == "__main__":
    unittest.main()
