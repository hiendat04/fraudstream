"""Tests for the TTL rule and the model's 51 inputs."""

import unittest
from datetime import UTC, datetime, timedelta

import pandas as pd

from fraudstream_ml.dataset import BATCH_FEATURE_VIEWS
from fraudstream_ml.features import prepare_features

from fraudstream_api.inference.features import (
    OnlineFeatures,
    history_found,
    model_inputs,
    plain,
    usable_features,
)
from fraudstream_api.inference.schemas import Transaction

# Two real rows from the data model version 2 trained on
# (iceberg.ml.training_data at snapshot 3023861480409485916).
WITH_HISTORY = {
    "transaction_id": "txn_000000000113", "customer_id": "cust_00103290",
    "merchant_id": "merch_00023570", "event_timestamp": "2026-06-29 16:46:40",
    "amount": 36.35, "channel": "atm", "city": "New York",
    "txn_count_7d": 1.0, "txn_count_30d": 2.0, "amount_sum_7d": 139.46,
    "amount_sum_30d": 181.73, "amount_avg_7d": 139.46, "amount_avg_30d": 90.87,
    "distinct_merchant_count_7d": 1.0, "distinct_merchant_count_30d": 2.0,
    "declined_txn_count_7d": 0.0, "fraud_txn_count_30d": 0.0, "total_orders_90d": 3.0,
    "merchant_category": "online_marketplace", "merchant_txn_count_1d": 1.0,
    "merchant_txn_count_7d": 1.0, "merchant_txn_count_30d": 2.0,
    "merchant_amount_sum_30d": 632.08, "merchant_amount_avg_30d": 316.04,
    "merchant_distinct_customer_count_1d": 1.0, "merchant_declined_txn_count_1d": 0.0,
    "merchant_fraud_rate_1d": 0.0, "merchant_prior_fraud_rate_30d": 0.0,
    "merchant_burst_ratio_1d_to_prior_30d": 1.0,
    "merchant_vs_category_amount_ratio_30d": 2.9244008513000836,
}

NO_CUSTOMER_HISTORY = {
    "transaction_id": "txn_000000000195", "customer_id": "cust_00121942",
    "merchant_id": "merch_00039131", "event_timestamp": "2026-06-24 19:16:18",
    "amount": 218.94, "channel": "card_present", "city": "New York",
    "txn_count_7d": None, "txn_count_30d": None, "amount_sum_7d": None,
    "amount_sum_30d": None, "amount_avg_7d": None, "amount_avg_30d": None,
    "distinct_merchant_count_7d": None, "distinct_merchant_count_30d": None,
    "declined_txn_count_7d": None, "fraud_txn_count_30d": None, "total_orders_90d": None,
    "merchant_category": "travel", "merchant_txn_count_1d": 1.0,
    "merchant_txn_count_7d": 2.0, "merchant_txn_count_30d": 3.0,
    "merchant_amount_sum_30d": 1592.46, "merchant_amount_avg_30d": 530.82,
    "merchant_distinct_customer_count_1d": 1.0, "merchant_declined_txn_count_1d": 0.0,
    "merchant_fraud_rate_1d": 0.0, "merchant_prior_fraud_rate_30d": 0.0,
    "merchant_burst_ratio_1d_to_prior_30d": 1.0,
    "merchant_vs_category_amount_ratio_30d": 1.4996609786416546,
}

TTLS = {
    "customer_rolling_features": timedelta(days=31),
    "customer_orders_90d_features": timedelta(days=91),
    "merchant_risk_features": timedelta(days=31),
}

PAYMENT_FIELDS = ("transaction_id", "customer_id", "merchant_id", "amount", "channel", "city")
FEATURE_NAMES = [name for names in BATCH_FEATURE_VIEWS.values() for name in names]


def transaction_from(row, when=None):
    """The payment part of a row, as a request would send it."""

    stamp = when or datetime.fromisoformat(row["event_timestamp"]).replace(tzinfo=UTC)
    return Transaction(**{name: row[name] for name in PAYMENT_FIELDS}, event_timestamp=stamp)


def online_from(row, value_age=timedelta(days=1), when=None):
    """What the online store would hold for a row: its features, each view a little older."""

    payment_time = when or datetime.fromisoformat(row["event_timestamp"]).replace(tzinfo=UTC)
    values = {name: row[name] for name in FEATURE_NAMES}
    event_times = {
        view: payment_time - value_age if any(row[name] is not None for name in names) else None
        for view, names in BATCH_FEATURE_VIEWS.items()
    }
    return OnlineFeatures(values=values, event_times=event_times)


def training_inputs(row):
    """Prepare a row the way the training pipeline did."""

    frame = pd.DataFrame([{name: float("nan") if value is None else value for name, value in row.items()}])
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"])
    prepared, names = prepare_features(frame)
    return {name: plain(prepared.iloc[0][name]) for name in names}


class ModelInputsTest(unittest.TestCase):
    def test_the_api_builds_exactly_what_training_built(self):
        """Serving and training share one piece of code, so their inputs must match exactly."""

        for row in (WITH_HISTORY, NO_CUSTOMER_HISTORY):
            with self.subTest(row=row["transaction_id"]):
                transaction = transaction_from(row)
                features = usable_features(online_from(row), transaction.event_timestamp, TTLS)

                served = model_inputs(transaction, features)
                trained = training_inputs(row)

                self.assertEqual(51, len(served))
                self.assertEqual(list(trained), list(served))
                self.assertEqual(trained, served)

    def test_a_view_older_than_its_ttl_counts_as_missing(self):
        transaction = transaction_from(WITH_HISTORY)
        online = online_from(WITH_HISTORY)
        online.event_times["customer_rolling_features"] = transaction.event_timestamp - timedelta(days=32)

        inputs = model_inputs(transaction, usable_features(online, transaction.event_timestamp, TTLS))

        for name in BATCH_FEATURE_VIEWS["customer_rolling_features"]:
            self.assertIsNone(inputs[name])
        self.assertFalse(inputs["customer_features_available"])
        self.assertEqual(3.0, inputs["total_orders_90d"])
        self.assertTrue(inputs["merchant_features_available"])

    def test_a_value_stamped_after_the_payment_counts_as_missing(self):
        """Training never joined a value stamped after the payment."""

        transaction = transaction_from(WITH_HISTORY)
        online = online_from(WITH_HISTORY)
        online.event_times["merchant_risk_features"] = transaction.event_timestamp + timedelta(hours=1)

        features = usable_features(online, transaction.event_timestamp, TTLS)

        for name in BATCH_FEATURE_VIEWS["merchant_risk_features"]:
            self.assertIsNone(features[name])
        self.assertEqual(1.0, features["txn_count_7d"])

    def test_age_is_measured_from_the_payment_not_from_today(self):
        """The store's data ends in June. Measuring from today would drop all of it."""

        when = datetime(2026, 6, 30, 20, 0, tzinfo=UTC)
        transaction = transaction_from(WITH_HISTORY, when=when)
        online = online_from(WITH_HISTORY, value_age=timedelta(days=10), when=when)

        features = usable_features(online, transaction.event_timestamp, TTLS)

        self.assertEqual(1.0, features["txn_count_7d"])
        self.assertEqual("online_marketplace", features["merchant_category"])

    def test_an_unknown_customer_is_scored_with_no_customer_history(self):
        transaction = transaction_from(NO_CUSTOMER_HISTORY)
        features = usable_features(online_from(NO_CUSTOMER_HISTORY), transaction.event_timestamp, TTLS)

        inputs = model_inputs(transaction, features)

        self.assertEqual(51, len(inputs))
        self.assertFalse(inputs["customer_features_available"])
        self.assertFalse(inputs["customer_orders_90d_available"])
        self.assertTrue(inputs["merchant_features_available"])

    def test_history_found_names_the_three_views(self):
        transaction = transaction_from(NO_CUSTOMER_HISTORY)
        inputs = model_inputs(
            transaction, usable_features(online_from(NO_CUSTOMER_HISTORY), transaction.event_timestamp, TTLS)
        )

        self.assertEqual(
            {
                "customer_features_available": False,
                "customer_orders_90d_available": False,
                "merchant_features_available": True,
            },
            history_found(inputs),
        )


if __name__ == "__main__":
    unittest.main()
