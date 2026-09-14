"""Contract tests for splitting work across workers and training with the native API."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from fraudstream_ml.distributed import (
    booster_to_classifier,
    native_params,
    shard_for_rank,
    train_distributed,
)


def _synthetic(rows: int = 600, positive_rate: float = 0.1, seed: int = 0):
    """Imbalanced set with a learnable signal and a null-heavy column, as the real data has."""

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


class ShardForRankTest(unittest.TestCase):
    def test_shards_are_disjoint_and_cover_every_row(self):
        """Every row lands on exactly one worker.

        If this breaks, each worker silently trains on a subset or a duplicate
        of the data and the run still succeeds, so nothing else would catch it.
        """

        features, _ = _synthetic(rows=101)
        for world_size in (1, 2, 3, 4):
            shards = [shard_for_rank(features, rank, world_size) for rank in range(world_size)]

            seen = pd.concat(shards).index
            self.assertEqual(
                len(seen), len(set(seen)), f"a row appears on two workers at world_size={world_size}"
            )
            self.assertEqual(
                sorted(seen),
                sorted(features.index),
                f"rows went missing at world_size={world_size}",
            )

    def test_shard_sizes_are_balanced(self):
        """No worker carries more than one row more than another."""

        features, _ = _synthetic(rows=101)
        for world_size in (2, 3, 4, 7):
            sizes = [len(shard_for_rank(features, r, world_size)) for r in range(world_size)]
            self.assertLessEqual(max(sizes) - min(sizes), 1, f"world_size={world_size} -> {sizes}")

    def test_features_and_labels_shard_in_step(self):
        """A row's features and its label have to end up on the same worker."""

        features, labels = _synthetic(rows=50)
        feature_shard = shard_for_rank(features, 1, 3)
        label_shard = shard_for_rank(labels, 1, 3)
        self.assertEqual(list(feature_shard.index), list(label_shard.index))

    def test_a_single_worker_gets_everything(self):
        """One worker is the degenerate case and must behave like no sharding at all."""

        features, _ = _synthetic(rows=50)
        pd.testing.assert_frame_equal(shard_for_rank(features, 0, 1), features)

    def test_rejects_a_rank_outside_the_world(self):
        features, _ = _synthetic(rows=50)
        with self.assertRaises(ValueError):
            shard_for_rank(features, 3, 3)
        with self.assertRaises(ValueError):
            shard_for_rank(features, -1, 3)


class NativeParamsTest(unittest.TestCase):
    def test_carries_the_sklearn_settings_across(self):
        """The rewrite to the native API must not quietly change how the model trains."""

        _, labels = _synthetic()
        params = native_params({"max_depth": 3, "learning_rate": 0.1}, labels)

        self.assertEqual(params["eta"], 0.1, "learning_rate must become eta")
        self.assertEqual(params["max_depth"], 3)
        self.assertEqual(params["eval_metric"], "aucpr")
        self.assertEqual(params["objective"], "binary:logistic")
        self.assertEqual(params["tree_method"], "hist")
        self.assertEqual(params["subsample"], 0.8)
        self.assertEqual(params["colsample_bytree"], 0.8)

    def test_does_not_leak_sklearn_only_names(self):
        """Names the native API does not understand are silently ignored by XGBoost."""

        _, labels = _synthetic()
        params = native_params({"learning_rate": 0.1, "n_estimators": 400}, labels)
        for name in ("learning_rate", "n_estimators", "random_state", "early_stopping_rounds"):
            self.assertNotIn(name, params)

    def test_scale_pos_weight_comes_from_the_shard_labels(self):
        """Each worker weights the imbalance it actually holds, not the whole set."""

        labels = pd.Series([0] * 90 + [1] * 10)
        params = native_params({}, labels)
        self.assertAlmostEqual(params["scale_pos_weight"], 9.0)

    def test_refuses_labels_with_no_positive_class(self):
        with self.assertRaises(ValueError):
            native_params({}, pd.Series([0, 0, 0]))


class BoosterRoundTripTest(unittest.TestCase):
    def test_booster_reloads_into_a_classifier_that_can_predict_proba(self):
        """Evaluation calls predict_proba, which a raw Booster does not have.

        Saving from the native API and loading back into the sklearn wrapper is
        what lets the existing evaluate() score a distributed model unchanged.
        """

        features, labels = _synthetic()
        booster = train_distributed(
            features,
            labels,
            features,
            labels,
            rank=0,
            world_size=1,
            params={"max_depth": 2, "learning_rate": 0.3},
            num_boost_round=5,
        )

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.json"
            booster.save_model(path)
            classifier = booster_to_classifier(path)

            scores = classifier.predict_proba(features)
            self.assertEqual(scores.shape, (len(features), 2))
            self.assertTrue(np.all((scores >= 0) & (scores <= 1)))
            np.testing.assert_allclose(
                scores[:, 1],
                booster.predict(__import__("xgboost").DMatrix(features)),
                rtol=1e-5,
                err_msg="the reloaded classifier must score identically to the booster",
            )


class TrainDistributedTest(unittest.TestCase):
    def test_single_worker_learns_the_signal(self):
        """One worker exercises the whole tracker and collective path end to end."""

        features, labels = _synthetic(rows=800)
        booster = train_distributed(
            features,
            labels,
            features,
            labels,
            rank=0,
            world_size=1,
            params={"max_depth": 3, "learning_rate": 0.2},
            num_boost_round=20,
        )

        import xgboost as xgb
        from sklearn.metrics import average_precision_score

        scores = booster.predict(xgb.DMatrix(features))
        base_rate = float(labels.mean())
        self.assertGreater(
            average_precision_score(labels, scores),
            base_rate * 2,
            "a model that cannot beat twice the base rate has not learned anything",
        )

    def test_rejects_labels_that_do_not_line_up_with_the_rows(self):
        """Catches the mix-up early instead of failing deep inside the collective."""

        features, labels = _synthetic(rows=100)
        with self.assertRaises(ValueError):
            train_distributed(
                features, labels.iloc[:50], features, labels, rank=0, world_size=1
            )

    def test_saves_the_model_when_given_a_path(self):
        features, labels = _synthetic(rows=200)
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "out" / "model.json"
            train_distributed(
                features,
                labels,
                features,
                labels,
                rank=0,
                world_size=1,
                params={"max_depth": 2, "learning_rate": 0.3},
                num_boost_round=5,
                model_path=path,
            )
            self.assertTrue(path.exists(), "rank 0 should have written the model")


if __name__ == "__main__":
    unittest.main()
