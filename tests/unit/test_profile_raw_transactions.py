"""Unit tests for the raw-data profile: distinct ids and the two schemas."""

from __future__ import annotations

import importlib.util
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.profile_raw_transactions import profile_raw_transactions

V1_COLUMNS = ["transaction_id", "customer_id"]
V2_COLUMNS = [*V1_COLUMNS, "device_id"]


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class ProfileRawTransactionsTest(TestCase):
    def test_profile_reports_distinct_ids_and_schema(self) -> None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("profile-test").getOrCreate()
        self.addCleanup(spark.stop)
        v1 = spark.createDataFrame([("t1", "c1"), ("t2", "c1"), ("t2", "c1")], V1_COLUMNS)
        v2 = spark.createDataFrame([("t3", "c2", "d1"), ("t4", "c3", "d1"), ("t5", "c3", "")], V2_COLUMNS)

        profile = profile_raw_transactions(v1, v2, stored_bytes=1234, file_count=2)

        self.assertEqual(profile["rows"], 6)
        self.assertEqual(profile["bytes"], 1234)
        self.assertEqual(profile["files"], 2)
        self.assertEqual(profile["cardinality"]["customer_id"]["distinct"], 3)
        self.assertEqual(profile["cardinality"]["customer_id"]["ratio"], 0.5)
        self.assertEqual(profile["cardinality"]["device_id"]["distinct"], 1)
        self.assertEqual(profile["schema"]["v1"], {"columns": V1_COLUMNS, "rows": 3})
        self.assertEqual(profile["schema"]["v2"], {"columns": V2_COLUMNS, "rows": 3})


if __name__ == "__main__":
    main()
