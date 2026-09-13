"""Contract tests for the labeled training frame pulled from the offline store."""

from __future__ import annotations

import unittest

import pandas as pd

from fraudstream_ml.dataset import (
    BATCH_FEATURE_REFS,
    BATCH_FEATURE_VIEWS,
    coverage_report,
    entity_dataframe_sql,
)


START = "2026-01-01"
END = "2026-07-01"


class EntityDataframeSqlTest(unittest.TestCase):
    def test_selects_the_label_from_the_label_table(self):
        """The label is joined in from its own table, which is the whole point of that table."""

        sql = entity_dataframe_sql(START, END)

        self.assertIn("iceberg.gold.transaction_labels", sql)
        self.assertIn("l.is_fraud", sql)

    def test_never_selects_the_fact_table_label(self):
        """fact_transactions carries its own is_fraud; reading it would bypass the label table.

        This is the leakage guard for the whole phase. `fact_transactions` is
        the table the entity rows come from and it has an `is_fraud` column
        sitting right there, so collapsing the join is a one-character
        temptation that no other test would catch.
        """

        sql = entity_dataframe_sql(START, END)

        self.assertNotIn("f.is_fraud", sql)

    def test_aliases_merchant_dim_id_to_the_entity_join_key(self):
        """The merchant feature view keys on merchant_id, built from merchant_dim_id."""

        sql = entity_dataframe_sql(START, END)

        self.assertIn("f.merchant_dim_id AS merchant_id", sql)

    def test_selects_the_request_time_attributes(self):
        """The label depends mostly on the transaction's own amount, channel and city."""

        sql = entity_dataframe_sql(START, END)

        self.assertIn("f.channel", sql)
        self.assertIn("f.city", sql)

    def test_casts_amount_to_a_float(self):
        """Iceberg decimals arrive as Python Decimal objects, which no estimator accepts."""

        sql = entity_dataframe_sql(START, END)

        self.assertIn("CAST(f.amount AS DOUBLE) AS amount", sql)

    def test_never_selects_the_transaction_outcome(self):
        """Status is decided after scoring, so training on it would inflate every metric."""

        sql = entity_dataframe_sql(START, END)

        for column in ("transaction_status", "is_approved", "is_declined", "is_reversed"):
            self.assertNotIn(column, sql, msg=column)

    def test_bounds_the_window(self):
        """Both ends of the window reach the query, so a split never silently reads everything."""

        sql = entity_dataframe_sql("2026-03-01", "2026-04-01")

        self.assertIn("2026-03-01 00:00:00", sql)
        self.assertIn("2026-04-01 00:00:00", sql)

    def test_rejects_an_end_that_is_not_after_the_start(self):
        """An inverted window returns zero rows, which is far harder to debug than an error."""

        with self.assertRaises(ValueError):
            entity_dataframe_sql("2026-04-01", "2026-03-01")

    def test_rejects_an_unparseable_timestamp(self):
        """Window bounds are interpolated into SQL, so they must be real timestamps."""

        with self.assertRaises(ValueError):
            entity_dataframe_sql("not-a-date", END)


class BatchFeatureRefsTest(unittest.TestCase):
    def test_refs_exclude_the_streaming_views(self):
        """The 5-minute views have a 1-hour TTL, so against months-old data they join to all-null.

        Feast reports no error for this -- it just returns a null column per
        streaming feature, which would look like a modeling problem rather
        than a retrieval one.
        """

        for ref in BATCH_FEATURE_REFS:
            self.assertNotIn("_5m_stream", ref, msg=ref)

    def test_refs_are_view_qualified(self):
        """Feast resolves features as `view:feature`, and every ref must name its view."""

        for ref in BATCH_FEATURE_REFS:
            view, separator, feature = ref.partition(":")
            self.assertEqual(separator, ":", msg=ref)
            self.assertIn(view, BATCH_FEATURE_VIEWS, msg=ref)
            self.assertIn(feature, BATCH_FEATURE_VIEWS[view], msg=ref)

    def test_every_view_contributes_at_least_one_ref(self):
        """A view registered but never requested is a silently missing feature group."""

        requested = {ref.partition(":")[0] for ref in BATCH_FEATURE_REFS}

        self.assertEqual(requested, set(BATCH_FEATURE_VIEWS))


class CoverageReportTest(unittest.TestCase):
    """Coverage is the headline property of this dataset, so it is measured, not assumed."""

    def _frame(self) -> pd.DataFrame:
        # Four rows: the customer view joined on two of them, the merchant
        # view on three, the 90-day view on one.
        return pd.DataFrame(
            {
                "txn_count_7d": [1, None, 3, None],
                "amount_sum_7d": [10.0, None, 30.0, None],
                "total_orders_90d": [5, None, None, None],
                "merchant_txn_count_1d": [2, 4, 6, None],
            }
        )

    def test_counts_the_share_of_rows_that_joined_each_view(self):
        report = coverage_report(self._frame())

        self.assertAlmostEqual(report["customer_rolling_features"], 0.5)
        self.assertAlmostEqual(report["customer_orders_90d_features"], 0.25)
        self.assertAlmostEqual(report["merchant_risk_features"], 0.75)

    def test_a_partly_null_row_still_counts_as_joined(self):
        """A joined row can carry a null feature; the view still produced data for it."""

        frame = pd.DataFrame(
            {
                "txn_count_7d": [None],
                "amount_sum_7d": [10.0],
                "total_orders_90d": [None],
                "merchant_txn_count_1d": [None],
            }
        )

        report = coverage_report(frame)

        self.assertAlmostEqual(report["customer_rolling_features"], 1.0)
        self.assertAlmostEqual(report["merchant_risk_features"], 0.0)

    def test_an_empty_frame_reports_zero_rather_than_nan(self):
        """A mean over no rows is NaN, which would propagate into the notebook's summary."""

        report = coverage_report(self._frame().iloc[:0])

        self.assertEqual(set(report), set(BATCH_FEATURE_VIEWS))
        for view, value in report.items():
            self.assertEqual(value, 0.0, msg=view)

    def test_a_frame_missing_a_view_entirely_is_an_error(self):
        """Losing a whole feature group is a retrieval bug, not a zero-coverage result."""

        with self.assertRaises(ValueError):
            coverage_report(pd.DataFrame({"txn_count_7d": [1]}))


if __name__ == "__main__":
    unittest.main()
