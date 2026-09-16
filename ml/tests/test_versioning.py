"""Contract tests for recording each training set as a version of an Iceberg table.

These run against a real Iceberg table in a temporary directory. The whole
claim of this module is about what a commit physically stores, and a mock
cannot tell you that.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pyspark.sql import SparkSession

from fraudstream_ml.versioning import (
    added_records,
    current_snapshot_id,
    read_at_snapshot,
    snapshot_history,
    write_snapshot,
)

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


def _frame(count: int, *, first_id: int = 0, amount: float = 10.0) -> pd.DataFrame:
    """A small stand-in for the retrieved training set."""

    day_offsets = [(first_id + i) % 90 for i in range(count)]
    return pd.DataFrame(
        {
            "transaction_id": [f"t{first_id + i:05d}" for i in range(count)],
            "event_timestamp": pd.Timestamp("2026-01-01") + pd.to_timedelta(day_offsets, unit="D"),
            "is_fraud": [(first_id + i) % 7 == 0 for i in range(count)],
            "amount": [amount + i for i in range(count)],
            "customer_txn_count_30d": [None if i % 3 == 0 else float(i) for i in range(count)],
        }
    )


class VersionedTrainingDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        warehouse = Path(cls._tmp.name).joinpath("warehouse").as_uri()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("VersioningTest")
            .config("spark.jars.packages", ICEBERG_PACKAGE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls._tmp.cleanup()

    def table(self) -> str:
        """A table of this test's own, so the tests cannot interfere with each other."""

        return f"iceberg.ml.{self.id().rsplit('.', 1)[-1]}"

    def rows_in(self, table: str) -> int:
        return self.spark.table(table).count()

    # ------------------------------------------------------------------

    def test_the_first_write_stores_every_row(self):
        table = self.table()
        write_snapshot(self.spark, _frame(100), table=table)

        self.assertEqual(100, self.rows_in(table))

    def test_writing_the_same_data_again_stores_nothing(self):
        """Re-running a window must not duplicate the rows or rewrite them.

        Iceberg still records a commit, which is fine and costs only metadata.
        What must not happen is that commit carrying data.
        """

        table = self.table()
        write_snapshot(self.spark, _frame(50), table=table)
        second = write_snapshot(self.spark, _frame(50), table=table)

        self.assertEqual(50, self.rows_in(table), "rows were duplicated")
        self.assertEqual(
            0,
            added_records(self.spark, table, second),
            "identical data was written again instead of being recognised",
        )

    def test_extending_the_window_stores_only_the_new_rows(self):
        """The whole point of the exercise.

        The second write contains every row of the first plus twenty more. If
        the commit stores 120 rows instead of 20, this is not incremental and
        each run costs a full copy of the training set.
        """

        table = self.table()
        write_snapshot(self.spark, _frame(100), table=table)
        second = write_snapshot(self.spark, _frame(120), table=table)

        self.assertEqual(120, self.rows_in(table))
        self.assertEqual(20, added_records(self.spark, table, second))

    def test_a_changed_value_updates_the_row_rather_than_adding_one(self):
        table = self.table()
        write_snapshot(self.spark, _frame(30, amount=10.0), table=table)
        write_snapshot(self.spark, _frame(30, amount=99.0), table=table)

        self.assertEqual(30, self.rows_in(table), "the changed rows were added alongside the old ones")
        amounts = {row["amount"] for row in self.spark.table(table).select("amount").collect()}
        self.assertNotIn(10.0, amounts, "the old value survived the update")

    def test_it_returns_the_snapshot_it_wrote(self):
        table = self.table()
        returned = write_snapshot(self.spark, _frame(10), table=table)

        self.assertEqual(current_snapshot_id(self.spark, table), returned)

    def test_an_earlier_snapshot_still_sees_the_data_of_its_own_run(self):
        """Without this, a recorded snapshot id would not reproduce anything."""

        table = self.table()
        first = write_snapshot(self.spark, _frame(40), table=table)
        write_snapshot(self.spark, _frame(60), table=table)

        self.assertEqual(60, self.rows_in(table))
        self.assertEqual(40, read_at_snapshot(self.spark, first, table=table).count())

    def test_the_history_reports_what_each_commit_added(self):
        table = self.table()
        write_snapshot(self.spark, _frame(100), table=table)
        write_snapshot(self.spark, _frame(130), table=table)

        history = snapshot_history(self.spark, table)

        self.assertEqual(2, len(history))
        self.assertEqual([100, 30], [row["added_records"] for row in history])
        self.assertEqual(130, history[-1]["total_records"])


if __name__ == "__main__":
    unittest.main()
