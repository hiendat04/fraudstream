"""Unit tests for Gold transaction table builds."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.gold.transactions import (
    GoldTransactionsConfig,
    build_gold_transactions,
)


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class GoldTransactionsTest(TestCase):
    """Tests for Silver to Gold transaction processing."""

    def test_builds_gold_tables_from_silver_transactions(self) -> None:
        """Gold should create facts, dimensions, aggregates, and features."""

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
                )
            )

            self.assertEqual(result.fact_transaction_count, 4)
            self.assertTrue((gold_dir / "_gold_transactions_summary.json").exists())

            fact_rows = {row["transaction_id"]: row for row in _read_parquet_rows(gold_dir / "fact_transactions")}
            self.assertEqual(set(fact_rows), {"txn_001", "txn_002", "txn_003", "txn_004"})
            self.assertEqual(fact_rows["txn_003"]["merchant_dim_id"], "UNKNOWN")
            self.assertEqual(fact_rows["txn_002"]["quality_issue_count"], 1)

            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_customer")), 2)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_account")), 2)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_merchant")), 3)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "dim_date")), 2)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_transaction_quality_issue")), 1)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_customer_daily")), 4)
            self.assertEqual(len(_read_parquet_rows(gold_dir / "fact_account_daily")), 3)

            training_rows = _read_parquet_rows(gold_dir / "feat_transaction_training")
            self.assertEqual(len(training_rows), 4)
            self.assertEqual({row["transaction_id"] for row in training_rows}, set(fact_rows))

            summary = _read_json(gold_dir / "_gold_transactions_summary.json")
            table_names = {table["table_name"] for table in summary["tables"]}
            self.assertIn("fact_transactions", table_names)
            self.assertIn("feat_customer_total_orders_90d", table_names)


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
