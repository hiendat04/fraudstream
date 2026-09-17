"""Tests for saving a trained model to MLflow.

These use a local SQLite database in a temporary folder. MLflow needs a real
database to track model versions. A plain folder will not work. No server is
needed.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from xgboost import XGBClassifier

from fraudstream_ml.registry import CHAMPION_ALIAS, log_training_run

PARAMS = {"max_depth": 3, "learning_rate": 0.1, "num_boost_round": 400, "num_nodes": 2}
METRICS = {"pr_auc": 0.0283, "roc_auc": 0.6584, "threshold": 0.626}


def _model() -> XGBClassifier:
    """A small real model, so the test saves a real model and not a fake one."""

    rng = np.random.default_rng(0)
    features = pd.DataFrame({"a": rng.random(120), "b": rng.random(120)})
    labels = (features["a"] > 0.5).astype(int)
    return XGBClassifier(max_depth=2, n_estimators=5).fit(features, labels)


class RegistryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.tracking_uri = f"sqlite:///{Path(cls._tmp.name) / 'mlflow.db'}"
        cls.model = _model()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def setUp(self):
        self.name = self.id().rsplit(".", 1)[-1]
        self.client = MlflowClient(tracking_uri=self.tracking_uri)

    def record(self, *, snapshot_id: int = 111, metrics: dict | None = None):
        """Save one run, the same way the last pipeline step does."""

        return log_training_run(
            self.model,
            params=PARAMS,
            metrics=METRICS if metrics is None else metrics,
            data_snapshot_id=snapshot_id,
            data_table="iceberg.ml.training_data",
            experiment=self.name,
            registered_model_name=self.name,
            tracking_uri=self.tracking_uri,
        )

    # ------------------------------------------------------------------

    def test_it_logs_the_settings_the_model_was_trained_with(self):
        recorded = self.record()

        logged = self.client.get_run(recorded.run_id).data.params
        self.assertEqual("3", logged["max_depth"])
        self.assertEqual("0.1", logged["learning_rate"])
        self.assertEqual("2", logged["num_nodes"])

    def test_it_logs_the_data_snapshot_it_trained_on(self):
        """This is what connects a model back to its data.

        Without it you cannot tell which rows a model version was trained on.
        """

        recorded = self.record(snapshot_id=987654321)

        logged = self.client.get_run(recorded.run_id).data.params
        self.assertEqual("987654321", logged["data_snapshot_id"])
        self.assertEqual("iceberg.ml.training_data", logged["data_table"])

    def test_it_logs_the_scores(self):
        recorded = self.record()

        logged = self.client.get_run(recorded.run_id).data.metrics
        self.assertAlmostEqual(0.0283, logged["pr_auc"], places=6)
        self.assertAlmostEqual(0.6584, logged["roc_auc"], places=6)

    def test_each_run_adds_a_version_rather_than_replacing_one(self):
        """A second run must add a version. It must not replace the first one."""

        first = self.record(snapshot_id=111)
        second = self.record(snapshot_id=222)

        self.assertEqual(1, first.model_version)
        self.assertEqual(2, second.model_version)

        versions = self.client.search_model_versions(f"name = '{self.name}'")
        self.assertEqual(2, len(versions))

    def test_the_two_versions_point_at_different_data(self):
        first = self.record(snapshot_id=111)
        second = self.record(snapshot_id=222)

        snapshots = {
            self.client.get_run(run.run_id).data.params["data_snapshot_id"]
            for run in (first, second)
        }
        self.assertEqual({"111", "222"}, snapshots)

    def test_it_does_not_promote_anything_by_itself(self):
        """Training must not change which model is used for predictions."""

        self.record()

        with self.assertRaises(MlflowException):
            self.client.get_model_version_by_alias(self.name, CHAMPION_ALIAS)

    def test_the_registered_model_loads_back_and_scores(self):
        """Checks a working model was saved, not just notes about one."""

        import mlflow

        recorded = self.record()
        mlflow.set_tracking_uri(self.tracking_uri)

        loaded = mlflow.pyfunc.load_model(f"models:/{self.name}/{recorded.model_version}")
        predictions = loaded.predict(pd.DataFrame({"a": [0.1, 0.9], "b": [0.5, 0.5]}))

        self.assertEqual(2, len(predictions))


if __name__ == "__main__":
    unittest.main()
