"""Unit tests for the watermark handling in the materialization job."""

from __future__ import annotations

import unittest
import unittest.mock
from datetime import datetime, timezone
from types import SimpleNamespace

from fraudstream_feast.materialize import materialize_features, plan_materialization


NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)
DATA_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeView:
    def __init__(self, name, most_recent_end_time=None, ttl=None):
        self.name = name
        self.most_recent_end_time = most_recent_end_time
        self.ttl = ttl


class FakeFeatureStore:
    """Records materialize calls instead of touching Feast, Spark, or Redis."""

    def __init__(self, views):
        self._views = {view.name: view for view in views}
        self.materialize_calls = []
        self.materialize_incremental_calls = []
        self.config = SimpleNamespace(offline_store=None)

    def get_feature_view(self, name):
        return self._views[name]

    def materialize(self, *, start_date, end_date, feature_views):
        self.materialize_calls.append(
            {"start_date": start_date, "end_date": end_date, "feature_views": list(feature_views)}
        )
        for name in feature_views:
            self._views[name].most_recent_end_time = end_date

    def materialize_incremental(self, *, end_date, feature_views):
        self.materialize_incremental_calls.append(
            {"end_date": end_date, "feature_views": list(feature_views)}
        )
        for name in feature_views:
            self._views[name].most_recent_end_time = end_date


class PlanMaterializationTest(unittest.TestCase):
    def test_never_materialized_views_get_an_explicit_full_window(self):
        """Feast's `now - ttl` fallback would skip data older than the TTL entirely."""

        plan = plan_materialization(
            [FakeView("customer_rolling_features")], start_timestamp=DATA_START, end_timestamp=NOW
        )

        self.assertEqual(plan["full"], ["customer_rolling_features"])
        self.assertEqual(plan["incremental"], [])

    def test_a_mixed_repo_splits_into_both_groups(self):
        """A newly added feature view must not force a full reload of the established ones."""

        plan = plan_materialization(
            [
                FakeView("customer_rolling_features", most_recent_end_time=datetime(2026, 6, 1, tzinfo=timezone.utc)),
                FakeView("merchant_risk_features"),
            ],
            start_timestamp=DATA_START,
            end_timestamp=NOW,
        )

        self.assertEqual(plan["full"], ["merchant_risk_features"])
        self.assertEqual(plan["incremental"], ["customer_rolling_features"])

    def test_end_before_start_is_rejected(self):
        """A backwards window silently materializes nothing, so fail loudly instead."""

        with self.assertRaises(ValueError):
            plan_materialization([FakeView("x")], start_timestamp=NOW, end_timestamp=DATA_START)


class MaterializeFeaturesDispatchTest(unittest.TestCase):
    def test_full_views_use_materialize_and_incremental_views_use_materialize_incremental(self):
        """Swapping the full/incremental branches must fail this test, not just silently move zero rows."""

        never_materialized = FakeView("customer_rolling_features")
        already_materialized = FakeView(
            "merchant_risk_features", most_recent_end_time=datetime(2026, 6, 1, tzinfo=timezone.utc)
        )
        fake_store = FakeFeatureStore([never_materialized, already_materialized])

        with unittest.mock.patch(
            "fraudstream_feast.materialize.FeatureStore", return_value=fake_store
        ), unittest.mock.patch(
            "fraudstream_feast.materialize._spot_check_online_store",
            return_value={"views": [], "requested": 0, "non_null": 0},
        ):
            materialize_features(
                repo_path="unused",
                start_timestamp=DATA_START,
                end_timestamp=NOW,
                feature_view_names=("customer_rolling_features", "merchant_risk_features"),
            )

        self.assertEqual(
            fake_store.materialize_calls,
            [{"start_date": DATA_START, "end_date": NOW, "feature_views": ["customer_rolling_features"]}],
        )
        self.assertEqual(
            fake_store.materialize_incremental_calls,
            [{"end_date": NOW, "feature_views": ["merchant_risk_features"]}],
        )


if __name__ == "__main__":
    unittest.main()
