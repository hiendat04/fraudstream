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

from fraudstream_serving.predictor import FraudPredictor

COLUMNS = ["amount", "customer_txn_count_30d", "event_hour"]


def _register_model(tracking_uri: str) -> str:
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


if __name__ == "__main__":
    unittest.main()
