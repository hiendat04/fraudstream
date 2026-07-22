"""Unit tests for Gold transaction table builds."""

from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.gold.offline_features import (
    OfflineFeatureConfig,
    build_offline_features,
)
from fraudstream.jobs.gold.transactions import (
    CORE_GOLD_TABLE_NAMES,
    GoldTransactionsConfig,
    _build_feat_merchant_risk_rolling,
    _build_merchant_category_rolling,
    build_gold_transactions,
)


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class GoldTransactionsTest(TestCase):
    """Tests for Silver to Gold transaction processing."""

    def test_builds_core_gold_then_offline_features_as_separate_jobs(self) -> None:
        """Core Gold and feature materialization should preserve transaction grain."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            silver_dir = root_dir / "silver" / "transactions"
            gold_dir = root_dir / "gold"
            _write_silver_fixture(silver_dir)

            result = build_gold_transactions(
                GoldTransactionsConfig(
                    silver_dir=silver_dir,
                    output_dir=gold_dir,
                    write_mode="overwrite",
                    processed_at=datetime.fromisoformat("2026-07-09T00:00:00+00:00"),
                    include_features=False,
                )
            )

            self.assertEqual(result.fact_transaction_count, 5)
            self.assertEqual(result.build_scope, "core")
            self.assertEqual(
                {table.table_name for table in result.table_results},
                set(CORE_GOLD_TABLE_NAMES),
            )
            self.assertFalse((gold_dir / "feat_transaction_training").exists())
            self.assertTrue((gold_dir / "_gold_transactions_summary.json").exists())

            fact_rows = {row["transaction_id"]: row for row in _read_parquet_rows(gold_dir / "fact_transactions")}
            self.assertEqual(set(fact_rows), {"txn_001", "txn_002", "txn_003", "txn_004", "txn_005"})
            self.assertEqual(fact_rows["txn_003"]["merchant_dim_id"], "UNKNOWN")
            self.assertEqual(fact_rows["txn_002"]["quality_issue_count"], 1)

            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_customer")), 2)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_account")), 2)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_merchant")), 3)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_date")), 3)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_transaction_quality_issue")), 1)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_customer_daily")), 5)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_account_daily")), 4)

            feature_result = build_offline_features(
                OfflineFeatureConfig(
                    gold_dir=gold_dir,
                    write_mode="overwrite",
                    processed_at=datetime.fromisoformat("2026-07-09T00:05:00+00:00"),
                )
            )
            self.assertEqual(feature_result.source_fact_transaction_count, 5)
            self.assertEqual(feature_result.training_row_count, 5)
            self.assertEqual(feature_result.training_distinct_transaction_count, 5)

            merchant_rows = _read_parquet_rows(gold_dir / "feat_merchant_risk_rolling")
            self.assertNotIn("UNKNOWN", {row["merchant_dim_id"] for row in merchant_rows})

            training_rows = _read_parquet_rows(gold_dir / "feat_transaction_training")
            training_by_id = {row["transaction_id"]: row for row in training_rows}
            self.assertEqual(len(training_rows), 5)
            self.assertEqual(set(training_by_id), set(fact_rows))
            self.assertTrue(training_by_id["txn_005"]["merchant_feature_available"])
            self.assertEqual(training_by_id["txn_005"]["merchant_txn_count_1d"], 2)
            self.assertEqual(training_by_id["txn_005"]["merchant_category_txn_count_1d"], 1)
            self.assertAlmostEqual(
                training_by_id["txn_005"]["merchant_vs_category_amount_ratio_30d"],
                1.25,
            )

            summary = _read_json(gold_dir / "_gold_transactions_summary.json")
            table_names = {table["table_name"] for table in summary["tables"]}
            self.assertIn("fact_transactions", table_names)
            self.assertNotIn("feat_merchant_risk_rolling", table_names)

            feature_summary = _read_json(gold_dir / "_offline_features_summary.json")
            feature_table_names = {
                table["table_name"] for table in feature_summary["tables"]
            }
            self.assertIn("feat_customer_total_orders_90d", feature_table_names)
            self.assertIn("feat_merchant_risk_rolling", feature_table_names)
            self.assertTrue(feature_summary["training_row_count_matches_fact"])

    def test_builds_merchant_burst_fraud_rate_and_category_features(self) -> None:
        """Merchant features should use calendar windows and omit the skewed unknown key."""

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("MerchantRiskFeatureTest")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            processed_at = datetime.fromisoformat("2026-07-09T00:00:00")
            start_date = date.fromisoformat("2026-01-01")
            merchant_rows = []
            category_rows = []
            for day_offset in range(31):
                feature_date = start_date + timedelta(days=day_offset)
                txn_count = 20 if day_offset == 30 else 2
                fraud_count = 4 if day_offset == 30 else 0
                merchant_rows.append(
                    {
                        "merchant_key": 1,
                        "merchant_dim_id": "merchant_001",
                        "date_key": int(feature_date.strftime("%Y%m%d")),
                        "feature_date": feature_date,
                        "merchant_category": "grocery",
                        "txn_count_1d": txn_count,
                        "amount_sum_1d": Decimal(txn_count * 10),
                        "amount_avg_1d": Decimal("10.00"),
                        "distinct_customer_count_1d": txn_count,
                        "declined_txn_count_1d": 0,
                        "fraud_txn_count_1d": fraud_count,
                        "fraud_rate_1d": fraud_count / txn_count,
                        "warning_txn_count_1d": 0,
                        "_gold_processed_at": processed_at,
                    }
                )
                category_rows.append(
                    {
                        "merchant_category": "grocery",
                        "feature_date": feature_date,
                        "category_txn_count_1d": txn_count * 2,
                        "category_amount_sum_1d": Decimal(txn_count * 20),
                        "category_fraud_txn_count_1d": fraud_count,
                    }
                )

            merchant_rows.append(
                {
                    **merchant_rows[-1],
                    "merchant_key": 99,
                    "merchant_dim_id": "UNKNOWN",
                }
            )
            merchant_daily = spark.createDataFrame(merchant_rows)
            category_daily = spark.createDataFrame(category_rows)
            category_rolling = _build_merchant_category_rolling(category_daily)
            features = _build_feat_merchant_risk_rolling(
                fact_merchant_daily=merchant_daily,
                merchant_category_rolling=category_rolling,
                processed_at=processed_at,
            ).collect()
        finally:
            spark.stop()

        self.assertEqual({row["merchant_dim_id"] for row in features}, {"merchant_001"})
        latest = next(row for row in features if row["feature_date"] == date.fromisoformat("2026-01-31"))
        self.assertEqual(latest["merchant_txn_count_1d"], 20)
        self.assertEqual(latest["merchant_txn_count_7d"], 32)
        self.assertEqual(latest["merchant_txn_count_30d"], 78)
        self.assertEqual(latest["merchant_txn_count_prior_30d"], 60)
        self.assertAlmostEqual(latest["merchant_burst_ratio_1d_to_prior_30d"], 10.0)
        self.assertEqual(latest["merchant_fraud_txn_count_30d"], 4)
        self.assertAlmostEqual(latest["merchant_prior_fraud_rate_30d"], 4 / 78)
        self.assertEqual(latest["merchant_category_txn_count_1d"], 40)
        self.assertEqual(latest["merchant_category_txn_count_30d"], 156)
        self.assertAlmostEqual(latest["merchant_category_prior_fraud_rate_30d"], 4 / 156)
        self.assertAlmostEqual(latest["merchant_vs_category_amount_ratio_30d"], 1.0)


def _write_silver_fixture(output_dir: Path) -> None:
    """Write a small Silver transaction Parquet fixture."""

    from pyspark.sql import SparkSession
    from pyspark.sql import types as spark_types

    spark = (
        SparkSession.builder.appName("GoldTransactionsTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        schema = spark_types.StructType(
            [
                spark_types.StructField("transaction_id", spark_types.StringType(), False),
                spark_types.StructField("account_id", spark_types.StringType(), False),
                spark_types.StructField("customer_id", spark_types.StringType(), False),
                spark_types.StructField("merchant_id", spark_types.StringType(), True),
                spark_types.StructField("merchant_category", spark_types.StringType(), True),
                spark_types.StructField("amount", spark_types.DecimalType(18, 2), False),
                spark_types.StructField("currency", spark_types.StringType(), False),
                spark_types.StructField("city", spark_types.StringType(), True),
                spark_types.StructField("channel", spark_types.StringType(), False),
                spark_types.StructField("transaction_status", spark_types.StringType(), False),
                spark_types.StructField("is_fraud", spark_types.BooleanType(), False),
                spark_types.StructField("event_time", spark_types.TimestampType(), False),
                spark_types.StructField("source_created_at", spark_types.TimestampType(), True),
                spark_types.StructField("arrival_delay_minutes", spark_types.DoubleType(), True),
                spark_types.StructField("device_id", spark_types.StringType(), True),
                spark_types.StructField("ip_address", spark_types.StringType(), True),
                spark_types.StructField("authentication_method", spark_types.StringType(), True),
                spark_types.StructField("risk_signal_version", spark_types.StringType(), True),
                spark_types.StructField("quality_status", spark_types.StringType(), False),
                spark_types.StructField("quality_issue_codes", spark_types.ArrayType(spark_types.StringType()), False),
                spark_types.StructField("duplicate_record_count", spark_types.IntegerType(), False),
                spark_types.StructField("dedup_rank", spark_types.IntegerType(), False),
                spark_types.StructField("_bronze_ingest_run_id", spark_types.StringType(), True),
                spark_types.StructField("_bronze_source_file_path", spark_types.StringType(), True),
                spark_types.StructField("_bronze_source_row_number", spark_types.LongType(), True),
                spark_types.StructField("_bronze_raw_record_hash", spark_types.StringType(), False),
                spark_types.StructField("_silver_processed_at", spark_types.TimestampType(), False),
                spark_types.StructField("event_date", spark_types.DateType(), False),
            ]
        )
        rows = [
            (
                "txn_001",
                "acct_001",
                "cust_001",
                "merch_001",
                "grocery",
                Decimal("10.00"),
                "USD",
                "Detroit",
                "online",
                "approved",
                False,
                datetime.fromisoformat("2026-01-01T10:00:00"),
                datetime.fromisoformat("2026-01-01T10:02:00"),
                2.0,
                "device_001",
                "10.0.0.1",
                "otp",
                "v2",
                "valid",
                [],
                1,
                1,
                "run_001",
                "source.csv",
                1,
                "hash_001",
                datetime.fromisoformat("2026-07-09T00:00:00"),
                datetime.fromisoformat("2026-01-01").date(),
            ),
            (
                "txn_002",
                "acct_001",
                "cust_001",
                "merch_002",
                "travel",
                Decimal("20.00"),
                "USD",
                "Chicago",
                "card_present",
                "declined",
                True,
                datetime.fromisoformat("2026-01-02T11:00:00"),
                datetime.fromisoformat("2026-01-02T12:30:00"),
                90.0,
                "device_002",
                "10.0.0.2",
                "pin",
                "v2",
                "warning",
                ["late_arrival"],
                2,
                1,
                "run_001",
                "source.csv",
                2,
                "hash_002",
                datetime.fromisoformat("2026-07-09T00:00:00"),
                datetime.fromisoformat("2026-01-02").date(),
            ),
            (
                "txn_004",
                "acct_001",
                "cust_002",
                "merch_001",
                "grocery",
                Decimal("15.00"),
                "USD",
                "Detroit",
                "online",
                "approved",
                False,
                datetime.fromisoformat("2026-01-01T13:00:00"),
                datetime.fromisoformat("2026-01-01T13:02:00"),
                2.0,
                "device_003",
                "10.0.0.3",
                "otp",
                "v2",
                "valid",
                [],
                1,
                1,
                "run_001",
                "source.csv",
                4,
                "hash_004",
                datetime.fromisoformat("2026-07-09T00:00:00"),
                datetime.fromisoformat("2026-01-01").date(),
            ),
            (
                "txn_003",
                "acct_002",
                "cust_002",
                None,
                "grocery",
                Decimal("5.00"),
                "USD",
                "Detroit",
                "atm",
                "reversed",
                False,
                datetime.fromisoformat("2026-01-02T12:00:00"),
                datetime.fromisoformat("2026-01-02T12:03:00"),
                3.0,
                None,
                None,
                None,
                None,
                "valid",
                [],
                1,
                1,
                "run_001",
                "source.csv",
                3,
                "hash_003",
                datetime.fromisoformat("2026-07-09T00:00:00"),
                datetime.fromisoformat("2026-01-02").date(),
            ),
            (
                "txn_005",
                "acct_002",
                "cust_002",
                "merch_001",
                "grocery",
                Decimal("30.00"),
                "USD",
                "Detroit",
                "online",
                "approved",
                False,
                datetime.fromisoformat("2026-01-03T10:00:00"),
                datetime.fromisoformat("2026-01-03T10:02:00"),
                2.0,
                "device_004",
                "10.0.0.4",
                "otp",
                "v2",
                "valid",
                [],
                1,
                1,
                "run_001",
                "source.csv",
                5,
                "hash_005",
                datetime.fromisoformat("2026-07-09T00:00:00"),
                datetime.fromisoformat("2026-01-03").date(),
            ),
        ]

        dataframe = spark.createDataFrame(rows, schema)
        dataframe.write.mode("overwrite").partitionBy("event_date").parquet(str(output_dir))
    finally:
        spark.stop()


def _read_parquet_rows(path: Path):
    """Read a Parquet directory and return rows before stopping Spark."""

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("GoldTransactionsTestReader")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        return spark.read.parquet(str(path)).collect()
    finally:
        spark.stop()


def _read_json(path: Path) -> dict:
    """Read a JSON artifact."""

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


if __name__ == "__main__":
    main()
