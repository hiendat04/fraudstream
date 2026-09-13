"""Contract tests for training, evaluation, threshold selection, and the saved bundle."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from fraudstream_ml.train import (
    evaluate,
    load_bundle,
    save_bundle,
    scale_pos_weight,
    select_threshold,
    train_baseline,
    train_xgboost,
    tune_xgboost,
)


def _synthetic(rows: int = 400, positive_rate: float = 0.1, seed: int = 0):
    """Imbalanced set with a learnable signal and a deliberately null-heavy column."""

    rng = np.random.default_rng(seed)
    labels = (rng.random(rows) < positive_rate).astype(int)
    signal = labels + rng.normal(0, 0.4, rows)
    features = pd.DataFrame(
        {
            "signal": signal,
            "noise": rng.normal(0, 1, rows),
            "sparse": np.where(rng.random(rows) < 0.5, np.nan, signal),
        }
    )
    return features, pd.Series(labels, name="is_fraud")


SMALL_GRID = (
    {"max_depth": 2, "learning_rate": 0.2},
    {"max_depth": 4, "learning_rate": 0.2},
)


class ScalePosWeightTest(unittest.TestCase):
    def test_matches_the_class_ratio(self):
        """XGBoost's imbalance lever is negatives divided by positives."""

        labels = pd.Series([0] * 95 + [1] * 5)

        self.assertAlmostEqual(scale_pos_weight(labels), 19.0)

    def test_is_one_when_balanced(self):
        self.assertAlmostEqual(scale_pos_weight(pd.Series([0, 1, 0, 1])), 1.0)


class EvaluateTest(unittest.TestCase):
    def setUp(self):
        features, labels = _synthetic()
        self.model = train_xgboost(
            features, labels, features, labels, n_estimators=25, early_stopping_rounds=5
        )
        self.features, self.labels = features, labels

    def test_reports_the_metrics_that_survive_class_imbalance(self):
        metrics = evaluate(self.model, self.features, self.labels, threshold=0.5)

        for key in ("pr_auc", "roc_auc", "precision", "recall", "f1"):
            self.assertIn(key, metrics, msg=key)
            self.assertGreaterEqual(metrics[key], 0.0, msg=key)

    def test_pr_auc_is_always_present_even_though_accuracy_is_reported(self):
        """Accuracy is reported only for contrast; a metrics dict without PR-AUC is the bug.

        At a 1.5% base rate an all-negative predictor scores 98.5% accuracy,
        so accuracy alone would make a useless model look excellent.
        """

        metrics = evaluate(self.model, self.features, self.labels)

        self.assertIn("pr_auc", metrics)
        self.assertIn("accuracy", metrics)

    def test_threshold_changes_the_positive_count(self):
        low = evaluate(self.model, self.features, self.labels, threshold=0.1)
        high = evaluate(self.model, self.features, self.labels, threshold=0.9)

        self.assertGreater(low["predicted_positives"], high["predicted_positives"])

    def test_records_the_threshold_it_scored_at(self):
        metrics = evaluate(self.model, self.features, self.labels, threshold=0.37)

        self.assertAlmostEqual(metrics["threshold"], 0.37)


class SelectThresholdTest(unittest.TestCase):
    def test_beats_the_default_half_on_skewed_scores(self):
        """Every positive scores below 0.5 here, so the default threshold predicts nothing."""

        rng = np.random.default_rng(7)
        labels = np.array([0] * 90 + [1] * 10)
        scores = np.concatenate([rng.uniform(0.0, 0.20, 90), rng.uniform(0.25, 0.45, 10)])

        chosen = select_threshold(labels, scores)

        def f1_at(threshold: float) -> float:
            predicted = (scores >= threshold).astype(int)
            true_positive = int(((predicted == 1) & (labels == 1)).sum())
            if predicted.sum() == 0 or true_positive == 0:
                return 0.0
            precision = true_positive / predicted.sum()
            recall = true_positive / labels.sum()
            return 2 * precision * recall / (precision + recall)

        self.assertGreater(f1_at(chosen), f1_at(0.5))

    def test_returns_a_usable_probability(self):
        rng = np.random.default_rng(3)
        labels = np.array([0] * 80 + [1] * 20)
        scores = np.concatenate([rng.uniform(0, 0.6, 80), rng.uniform(0.4, 1.0, 20)])

        chosen = select_threshold(labels, scores)

        self.assertGreaterEqual(chosen, 0.0)
        self.assertLessEqual(chosen, 1.0)


class TrainingTest(unittest.TestCase):
    def test_xgboost_learns_a_separable_signal(self):
        train_x, train_y = _synthetic(seed=1)
        val_x, val_y = _synthetic(seed=2)

        model = train_xgboost(train_x, train_y, val_x, val_y, n_estimators=60)
        metrics = evaluate(model, val_x, val_y)

        self.assertGreater(metrics["pr_auc"], 0.5)

    def test_xgboost_accepts_nulls_without_imputation(self):
        """The null column is the whole reason this model was chosen over a linear one."""

        train_x, train_y = _synthetic(seed=1)
        val_x, val_y = _synthetic(seed=2)

        self.assertTrue(train_x["sparse"].isna().any())

        model = train_xgboost(train_x, train_y, val_x, val_y, n_estimators=25)

        self.assertEqual(len(model.predict_proba(val_x)), len(val_x))

    def test_baseline_imputes_so_logistic_regression_can_run(self):
        """Logistic regression cannot take NaN, so its pipeline carries an imputer."""

        train_x, train_y = _synthetic(seed=1)
        val_x, val_y = _synthetic(seed=2)

        model = train_baseline(train_x, train_y)
        metrics = evaluate(model, val_x, val_y)

        self.assertGreater(metrics["pr_auc"], 0.0)

    def test_tuning_reports_every_config_and_returns_the_best(self):
        train_x, train_y = _synthetic(seed=1)
        val_x, val_y = _synthetic(seed=2)

        model, results = tune_xgboost(
            train_x, train_y, val_x, val_y, grid=SMALL_GRID, n_estimators=25
        )

        self.assertEqual(len(results), len(SMALL_GRID))
        self.assertIn("validation_pr_auc", results.columns)
        best = results.sort_values("validation_pr_auc", ascending=False).iloc[0]
        self.assertEqual(model.get_params()["max_depth"], best["max_depth"])

    def test_tuning_ranks_by_validation_not_training_score(self):
        """Selecting on training score would pick the most overfit config every time."""

        train_x, train_y = _synthetic(seed=1)
        val_x, val_y = _synthetic(seed=2)

        _, results = tune_xgboost(
            train_x, train_y, val_x, val_y, grid=SMALL_GRID, n_estimators=25
        )

        self.assertEqual(results["validation_pr_auc"].is_monotonic_decreasing, True)


class BundleTest(unittest.TestCase):
    def _trained(self):
        features, labels = _synthetic(seed=1)
        model = train_xgboost(features, labels, features, labels, n_estimators=25)
        return model, features, labels

    def test_round_trips_and_scores_identically(self):
        model, features, _ = self._trained()
        before = model.predict_proba(features)[:, 1]

        with TemporaryDirectory() as directory:
            path = save_bundle(
                Path(directory) / "model.joblib",
                model=model,
                feature_names=list(features.columns),
                threshold=0.42,
                metrics={"pr_auc": 0.9},
            )
            bundle = load_bundle(path)

        after = bundle["model"].predict_proba(features)[:, 1]

        np.testing.assert_allclose(before, after)

    def test_carries_the_serving_contract(self):
        """Column order and threshold live with the model, so serving cannot guess them wrong."""

        model, features, _ = self._trained()

        with TemporaryDirectory() as directory:
            path = save_bundle(
                Path(directory) / "model.joblib",
                model=model,
                feature_names=list(features.columns),
                threshold=0.42,
                metrics={"pr_auc": 0.9},
            )
            bundle = load_bundle(path)

        self.assertEqual(bundle["feature_names"], list(features.columns))
        self.assertAlmostEqual(bundle["threshold"], 0.42)
        self.assertEqual(bundle["metrics"], {"pr_auc": 0.9})
        self.assertIn("trained_at", bundle)

    def test_creates_the_parent_directory(self):
        model, features, _ = self._trained()

        with TemporaryDirectory() as directory:
            path = save_bundle(
                Path(directory) / "nested" / "model.joblib",
                model=model,
                feature_names=list(features.columns),
                threshold=0.5,
                metrics={},
            )

            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
