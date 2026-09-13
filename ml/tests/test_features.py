"""Contract tests for feature preparation."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from fraudstream_ml.features import (
    IDENTIFIER_COLUMNS,
    LABEL_COLUMN,
    MERCHANT_CATEGORIES,
    prepare_features,
)


def _frame() -> pd.DataFrame:
    """Three rows: one fully covered, one missing customer features, one missing merchant."""

    return pd.DataFrame(
        {
            "transaction_id": ["txn_0", "txn_1", "txn_2"],
            "customer_id": ["cust_0", "cust_1", "cust_2"],
            "merchant_id": ["merch_0", "merch_1", "merch_2"],
            "event_timestamp": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "is_fraud": [0, 1, 0],
            "txn_count_7d": [3.0, np.nan, 5.0],
            "amount_sum_7d": [30.0, np.nan, 50.0],
            "total_orders_90d": [9.0, np.nan, 11.0],
            "merchant_txn_count_1d": [7.0, 8.0, np.nan],
            "merchant_fraud_rate_1d": [0.1, 0.2, np.nan],
            "merchant_category": ["grocery", "travel", None],
        }
    )


class AvailabilityFlagTest(unittest.TestCase):
    def test_flags_mark_missing_feature_groups(self):
        """Whether a view joined at all is itself predictive, so it becomes a column.

        Roughly 70% of transactions have no customer-rolling snapshot inside
        the view's TTL. Dropping those rows would bias the set toward
        high-activity customers; imputing them would erase the fact that the
        customer has no recent history.
        """

        prepared, _ = prepare_features(_frame())

        self.assertEqual(list(prepared["customer_features_available"]), [True, False, True])
        self.assertEqual(list(prepared["merchant_features_available"]), [True, True, False])
        self.assertEqual(list(prepared["customer_orders_90d_available"]), [True, False, True])

    def test_nulls_are_preserved_not_imputed(self):
        """XGBoost learns a default split direction per node, so NaN must survive."""

        prepared, _ = prepare_features(_frame())

        self.assertTrue(pd.isna(prepared.loc[1, "txn_count_7d"]))
        self.assertTrue(pd.isna(prepared.loc[2, "merchant_txn_count_1d"]))


class FeatureListTest(unittest.TestCase):
    def test_identifier_and_label_columns_are_excluded(self):
        """Training on an identifier memorizes rows instead of learning behaviour."""

        _, feature_names = prepare_features(_frame())

        for column in (*IDENTIFIER_COLUMNS, LABEL_COLUMN):
            self.assertNotIn(column, feature_names, msg=column)

    def test_raw_categorical_column_is_not_a_feature(self):
        """merchant_category is encoded; the raw string column must not reach the model."""

        _, feature_names = prepare_features(_frame())

        self.assertNotIn("merchant_category", feature_names)

    def test_feature_order_is_stable_across_calls(self):
        """The saved bundle records this order as a serving contract."""

        _, first = prepare_features(_frame())
        _, second = prepare_features(_frame())

        self.assertEqual(first, second)

    def test_feature_order_does_not_depend_on_input_column_order(self):
        """A reordered frame must not silently produce a differently-ordered feature vector."""

        frame = _frame()
        shuffled = frame[list(reversed(frame.columns))]

        _, from_original = prepare_features(frame)
        _, from_shuffled = prepare_features(shuffled)

        self.assertEqual(from_original, from_shuffled)

    def test_every_feature_column_exists_on_the_prepared_frame(self):
        prepared, feature_names = prepare_features(_frame())

        for name in feature_names:
            self.assertIn(name, prepared.columns, msg=name)

    def test_label_and_timestamp_survive_on_the_frame(self):
        """Splitting and scoring still need them, even though the model never sees them."""

        prepared, _ = prepare_features(_frame())

        self.assertIn(LABEL_COLUMN, prepared.columns)
        self.assertIn("event_timestamp", prepared.columns)


class CategoryEncodingTest(unittest.TestCase):
    def test_one_hot_column_per_known_category(self):
        prepared, feature_names = prepare_features(_frame())

        for category in MERCHANT_CATEGORIES:
            column = f"merchant_category_{category}"
            self.assertIn(column, feature_names, msg=column)
            self.assertIn(column, prepared.columns, msg=column)

    def test_encodes_the_row_s_own_category(self):
        prepared, _ = prepare_features(_frame())

        self.assertEqual(prepared.loc[0, "merchant_category_grocery"], 1)
        self.assertEqual(prepared.loc[0, "merchant_category_travel"], 0)
        self.assertEqual(prepared.loc[1, "merchant_category_travel"], 1)

    def test_missing_category_encodes_as_all_zero(self):
        """A row whose merchant view never joined has no category; the flag carries that."""

        prepared, _ = prepare_features(_frame())

        indicators = [prepared.loc[2, f"merchant_category_{c}"] for c in MERCHANT_CATEGORIES]

        self.assertEqual(set(indicators), {0})
        self.assertFalse(bool(prepared.loc[2, "merchant_features_available"]))

    def test_unknown_category_does_not_add_a_column(self):
        """The encoded column set is a fixed contract, not whatever the batch happened to contain."""

        frame = _frame()
        frame.loc[0, "merchant_category"] = "brand_new_category"

        prepared, feature_names = prepare_features(frame)

        self.assertNotIn("merchant_category_brand_new_category", prepared.columns)
        indicators = [prepared.loc[0, f"merchant_category_{c}"] for c in MERCHANT_CATEGORIES]
        self.assertEqual(set(indicators), {0})
        self.assertEqual(len([n for n in feature_names if n.startswith("merchant_category_")]),
                         len(MERCHANT_CATEGORIES))


if __name__ == "__main__":
    unittest.main()
