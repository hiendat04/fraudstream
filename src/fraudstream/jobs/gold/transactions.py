"""Build Gold transaction tables from Silver transaction Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from fraudstream.jobs.bronze.ingest_transactions import DEFAULT_MASTER, SUPPORTED_WRITE_MODES
from fraudstream.jobs.postgres.publish import GOLD_TABLE_COLUMNS
from fraudstream.jobs.silver.transactions import DEFAULT_OUTPUT_DIR as DEFAULT_SILVER_DIR


APP_NAME = "FraudStreamGoldTransactions"
DEFAULT_OUTPUT_DIR = Path("data/gold")
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_gold_transactions_summary.json"
UNKNOWN_MERCHANT_DIM_ID = "UNKNOWN"
DEFAULT_COUNTRY_CODE = "US"

QUALITY_ISSUE_DEFINITIONS = (
    ("missing_transaction_id", "quarantine", "silver", "Transaction ID is missing or blank."),
    ("missing_account_id", "quarantine", "silver", "Account ID is missing or blank."),
    ("missing_customer_id", "quarantine", "silver", "Customer ID is missing or blank."),
    ("invalid_amount", "quarantine", "silver", "Amount cannot be parsed as a valid decimal value."),
    ("negative_amount", "quarantine", "silver", "Amount is negative."),
    ("invalid_event_time", "quarantine", "silver", "Event timestamp cannot be parsed."),
    ("invalid_fraud_label", "quarantine", "silver", "Fraud label is not a valid boolean flag."),
    ("unexpected_currency", "warning", "silver", "Currency is outside the expected generated values."),
    ("unexpected_channel", "warning", "silver", "Channel is outside the expected generated values."),
    ("unexpected_status", "warning", "silver", "Transaction status is outside the expected values."),
    ("negative_arrival_delay", "warning", "silver", "Source creation time is before event time."),
    ("late_arrival", "warning", "silver", "Source creation time is more than the expected delay after event time."),
    ("missing_evolved_value", "warning", "silver", "A schema-evolved field is missing on a row where it is expected."),
)


@dataclass(frozen=True)
class GoldTransactionsConfig:
    """Runtime settings for the Gold transaction build."""

    silver_dir: Path = DEFAULT_SILVER_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    processed_at: datetime | None = None

    def validate(self) -> None:
        """Raise when the Gold build cannot run with this config."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        if not self.silver_dir.exists():
            raise FileNotFoundError(f"silver_dir does not exist: {self.silver_dir}")


@dataclass(frozen=True)
class GoldTableResult:
    """Row count for one Gold output table."""

    table_name: str
    output_path: Path
    row_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable table result."""

        return {
            "table_name": self.table_name,
            "output_path": str(self.output_path),
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class GoldTransactionsResult:
    """Summary of one Gold transaction build."""

    silver_dir: Path
    output_dir: Path
    write_mode: str
    spark_version: str
    processed_at: str
    completed_at: str
    table_results: tuple[GoldTableResult, ...]

    @property
    def fact_transaction_count(self) -> int:
        """Return the row count for the transaction fact table."""

        for table in self.table_results:
            if table.table_name == "fact_transactions":
                return table.row_count
        return 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable job summary."""

        return {
            "silver_dir": str(self.silver_dir),
            "output_dir": str(self.output_dir),
            "write_mode": self.write_mode,
            "spark_version": self.spark_version,
            "processed_at": self.processed_at,
            "completed_at": self.completed_at,
            "fact_transaction_count": self.fact_transaction_count,
            "tables": [table.to_dict() for table in self.table_results],
        }


@dataclass(frozen=True)
class GoldBuildFrames:
    """DataFrames produced by the Gold build before writing."""

    dim_date: Any
    dim_city: Any
    dim_channel: Any
    dim_quality_issue: Any
    dim_merchant_category: Any
    dim_customer: Any
    dim_account: Any
    dim_merchant: Any
    fact_transactions: Any
    fact_transaction_quality_issue: Any
    fact_customer_daily: Any
    fact_account_daily: Any
    fact_merchant_daily: Any
    fact_city_category_daily: Any
    fact_device_ip_daily: Any
    feat_customer_rolling: Any
    feat_customer_total_orders_90d: Any
    feat_transaction_training: Any


def build_gold_transactions(config: GoldTransactionsConfig) -> GoldTransactionsResult:
    """Build Gold transaction facts, dimensions, aggregates, and features."""

    config.validate()
    spark = _build_spark_session(config.master)
    persisted_frames: list[Any] = []
    try:
        processed_at = config.processed_at or datetime.now(UTC)
        silver_dataframe = _prepare_silver_dataframe(spark.read.parquet(str(config.silver_dir)))
        frames = _build_gold_frames(spark, silver_dataframe, processed_at)

        persisted_frames.extend(
            [
                frames.dim_customer,
                frames.dim_account,
                frames.dim_merchant,
                frames.fact_transactions,
                frames.fact_customer_daily,
                frames.fact_account_daily,
                frames.fact_merchant_daily,
            ]
        )
        for dataframe in persisted_frames:
            dataframe.persist()

        table_results = _write_gold_tables(frames, config)
        result = GoldTransactionsResult(
            silver_dir=config.silver_dir,
            output_dir=config.output_dir,
            write_mode=config.write_mode,
            spark_version=spark.version,
            processed_at=_to_utc_string(processed_at),
            completed_at=_to_utc_string(datetime.now(UTC)),
            table_results=tuple(table_results),
        )
        _write_summary(result, config.output_dir)
        return result
    finally:
        for dataframe in persisted_frames:
            dataframe.unpersist(blocking=False)
        spark.stop()


def _build_spark_session(master: str) -> Any:
    """Create a Spark session for Gold processing."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark`, then retry this command."
        ) from exc

    return (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _prepare_silver_dataframe(silver_dataframe: Any) -> Any:
    """Add shared Gold helper columns to the Silver transaction DataFrame."""

    from pyspark.sql import functions as spark_functions

    return (
        silver_dataframe.withColumn(
            "merchant_dim_id",
            spark_functions.coalesce(spark_functions.col("merchant_id"), spark_functions.lit(UNKNOWN_MERCHANT_DIM_ID)),
        )
        .withColumn("date_key", spark_functions.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("transaction_count", spark_functions.lit(1).cast("smallint"))
        .withColumn("is_approved", spark_functions.col("transaction_status") == spark_functions.lit("approved"))
        .withColumn("is_declined", spark_functions.col("transaction_status") == spark_functions.lit("declined"))
        .withColumn("is_reversed", spark_functions.col("transaction_status") == spark_functions.lit("reversed"))
        .withColumn("quality_issue_count", spark_functions.size("quality_issue_codes").cast("int"))
    )


def _build_gold_frames(spark: Any, silver_dataframe: Any, processed_at: datetime) -> GoldBuildFrames:
    """Create all Gold DataFrames without writing them."""

    dim_date = _build_dim_date(silver_dataframe)
    dim_city = _build_dim_city(silver_dataframe, processed_at)
    dim_channel = _build_dim_channel(silver_dataframe, processed_at)
    dim_quality_issue = _build_dim_quality_issue(spark, processed_at)
    dim_merchant_category = _build_dim_merchant_category(silver_dataframe, processed_at)
    dim_customer = _build_dim_customer(silver_dataframe, dim_city, processed_at)
    dim_account = _build_dim_account(silver_dataframe, dim_customer, processed_at)
    dim_merchant = _build_dim_merchant(silver_dataframe, dim_city, dim_merchant_category, processed_at)
    fact_transactions = _build_fact_transactions(
        silver_dataframe=silver_dataframe,
        dim_customer=dim_customer,
        dim_account=dim_account,
        dim_merchant=dim_merchant,
        dim_channel=dim_channel,
        dim_city=dim_city,
        dim_merchant_category=dim_merchant_category,
        processed_at=processed_at,
    )
    fact_transaction_quality_issue = _build_fact_transaction_quality_issue(fact_transactions)
    fact_customer_daily = _build_fact_customer_daily(fact_transactions)
    fact_account_daily = _build_fact_account_daily(fact_transactions)
    fact_merchant_daily = _build_fact_merchant_daily(fact_transactions)
    fact_city_category_daily = _build_fact_city_category_daily(fact_transactions)
    fact_device_ip_daily = _build_fact_device_ip_daily(silver_dataframe, fact_transactions)
    feat_customer_rolling = _build_feat_customer_rolling(fact_customer_daily, processed_at)
    feat_customer_total_orders_90d = _build_feat_customer_total_orders_90d(fact_customer_daily, processed_at)
    feat_transaction_training = _build_feat_transaction_training(
        fact_transactions=fact_transactions,
        feat_customer_rolling=feat_customer_rolling,
        fact_account_daily=fact_account_daily,
        fact_merchant_daily=fact_merchant_daily,
        processed_at=processed_at,
    )

    return GoldBuildFrames(
        dim_date=dim_date,
        dim_city=dim_city,
        dim_channel=dim_channel,
        dim_quality_issue=dim_quality_issue,
        dim_merchant_category=dim_merchant_category,
        dim_customer=dim_customer,
        dim_account=dim_account,
        dim_merchant=dim_merchant,
        fact_transactions=fact_transactions,
        fact_transaction_quality_issue=fact_transaction_quality_issue,
        fact_customer_daily=fact_customer_daily,
        fact_account_daily=fact_account_daily,
        fact_merchant_daily=fact_merchant_daily,
        fact_city_category_daily=fact_city_category_daily,
        fact_device_ip_daily=fact_device_ip_daily,
        feat_customer_rolling=feat_customer_rolling,
        feat_customer_total_orders_90d=feat_customer_total_orders_90d,
        feat_transaction_training=feat_transaction_training,
    )


def _build_dim_date(silver_dataframe: Any) -> Any:
    """Build the calendar date dimension."""

    from pyspark.sql import functions as spark_functions

    return (
        silver_dataframe.select("event_date")
        .distinct()
        .where(spark_functions.col("event_date").isNotNull())
        .withColumn("date_key", spark_functions.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("year", spark_functions.year("event_date").cast("int"))
        .withColumn("quarter", spark_functions.quarter("event_date").cast("int"))
        .withColumn("month", spark_functions.month("event_date").cast("int"))
        .withColumn("month_name", spark_functions.date_format("event_date", "MMMM"))
        .withColumn("day_of_month", spark_functions.dayofmonth("event_date").cast("int"))
        .withColumn("day_of_week", spark_functions.dayofweek("event_date").cast("int"))
        .withColumn("day_name", spark_functions.date_format("event_date", "EEEE"))
        .withColumn("week_start_date", spark_functions.expr("date_sub(event_date, dayofweek(event_date) - 1)"))
        .withColumn("month_start_date", spark_functions.trunc("event_date", "MM").cast("date"))
        .withColumn("is_weekend", spark_functions.dayofweek("event_date").isin(1, 7))
        .select(*GOLD_TABLE_COLUMNS["dim_date"])
    )


def _build_dim_city(silver_dataframe: Any, processed_at: datetime) -> Any:
    """Build the city lookup dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("city")
    return (
        silver_dataframe.select("city")
        .distinct()
        .where(spark_functions.col("city").isNotNull())
        .withColumn("city_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("state_code", spark_functions.lit(None).cast("string"))
        .withColumn("country_code", spark_functions.lit(DEFAULT_COUNTRY_CODE))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["dim_city"])
    )


def _build_dim_channel(silver_dataframe: Any, processed_at: datetime) -> Any:
    """Build the channel lookup dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("channel")
    channel_group = (
        spark_functions.when(
            spark_functions.col("channel").isin("online", "mobile_wallet"),
            spark_functions.lit("digital"),
        )
        .when(spark_functions.col("channel") == spark_functions.lit("card_present"), spark_functions.lit("card"))
        .when(spark_functions.col("channel") == spark_functions.lit("atm"), spark_functions.lit("cash"))
        .otherwise(spark_functions.lit("other"))
    )
    description = spark_functions.concat(
        spark_functions.initcap(spark_functions.regexp_replace("channel", "_", " ")),
        spark_functions.lit(" transaction channel"),
    )

    return (
        silver_dataframe.select("channel")
        .distinct()
        .where(spark_functions.col("channel").isNotNull())
        .withColumn("channel_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("channel_group", channel_group)
        .withColumn("is_digital", spark_functions.col("channel").isin("online", "mobile_wallet"))
        .withColumn("is_card_present", spark_functions.col("channel") == spark_functions.lit("card_present"))
        .withColumn("description", description)
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["dim_channel"])
    )


def _build_dim_quality_issue(spark: Any, processed_at: datetime) -> Any:
    """Build the quality issue lookup dimension."""

    from pyspark.sql import functions as spark_functions

    return (
        spark.createDataFrame(
            QUALITY_ISSUE_DEFINITIONS,
            ["quality_issue_code", "severity", "layer_origin", "description"],
        )
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["dim_quality_issue"])
    )

def _build_dim_merchant_category(silver_dataframe: Any, processed_at: datetime) -> Any:
    """Build the merchant category lookup dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("merchant_category")
    category_group = (
        spark_functions.when(
            spark_functions.col("merchant_category").contains("online"),
            spark_functions.lit("digital_commerce"),
        )
        .when(
            spark_functions.col("merchant_category").isin("travel", "airline", "hotel"),
            spark_functions.lit("travel"),
        )
        .otherwise(spark_functions.lit("general_commerce"))
    )

    return (
        silver_dataframe.select("merchant_category")
        .distinct()
        .where(spark_functions.col("merchant_category").isNotNull())
        .withColumn("merchant_category_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("category_group", category_group)
        .withColumn(
            "description",
            spark_functions.concat(
                spark_functions.initcap(spark_functions.regexp_replace("merchant_category", "_", " ")),
                spark_functions.lit(" merchant category"),
            ),
        )
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["dim_merchant_category"])
    )


def _build_dim_customer(silver_dataframe: Any, dim_city: Any, processed_at: datetime) -> Any:
    """Build the SCD2 customer dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("customer_id")
    primary_city = _most_common_value(silver_dataframe, "customer_id", "city", "primary_city")
    metrics = (
        silver_dataframe.groupBy("customer_id")
        .agg(
            spark_functions.min("event_time").alias("first_seen_at"),
            spark_functions.max("event_time").alias("last_seen_at"),
            spark_functions.min("event_date").alias("first_event_date"),
            spark_functions.max("event_date").alias("last_event_date"),
            spark_functions.countDistinct("account_id").cast("long").alias("account_count"),
            spark_functions.count("*").cast("long").alias("lifetime_transaction_count"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("lifetime_amount"),
            spark_functions.round(spark_functions.avg("amount"), 2)
            .cast("decimal(18,2)")
            .alias("average_transaction_amount"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_transaction_count"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_transaction_count"
            ),
        )
        .join(primary_city, "customer_id", "left")
        .join(dim_city.select(spark_functions.col("city").alias("primary_city"), "city_key"), "primary_city", "left")
        .withColumnRenamed("city_key", "primary_city_key")
        .withColumn("customer_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("fraud_rate", _safe_rate("fraud_transaction_count", "lifetime_transaction_count"))
        .withColumn("valid_from_ts", spark_functions.col("first_seen_at"))
        .withColumn("valid_to_ts", spark_functions.lit(None).cast("timestamp"))
        .withColumn("is_current", spark_functions.lit(True))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
    )
    return metrics.select(*GOLD_TABLE_COLUMNS["dim_customer"])


def _build_dim_account(silver_dataframe: Any, dim_customer: Any, processed_at: datetime) -> Any:
    """Build the SCD2 account dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("account_id")
    account_customer = _most_common_value(silver_dataframe, "account_id", "customer_id", "customer_id")
    metrics = (
        silver_dataframe.groupBy("account_id")
        .agg(
            spark_functions.countDistinct("customer_id").cast("long").alias("customer_count"),
            spark_functions.min("event_time").alias("first_seen_at"),
            spark_functions.max("event_time").alias("last_seen_at"),
            spark_functions.count("*").cast("long").alias("transaction_count"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("lifetime_amount"),
            spark_functions.countDistinct("merchant_dim_id").cast("long").alias("distinct_merchant_count"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_transaction_count"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_transaction_count"
            ),
        )
        .join(account_customer, "account_id", "left")
        .join(dim_customer.where("is_current").select("customer_key", "customer_id"), "customer_id", "left")
        .withColumn("account_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("valid_from_ts", spark_functions.col("first_seen_at"))
        .withColumn("valid_to_ts", spark_functions.lit(None).cast("timestamp"))
        .withColumn("is_current", spark_functions.lit(True))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
    )
    return metrics.select(*GOLD_TABLE_COLUMNS["dim_account"])


def _build_dim_merchant(
    silver_dataframe: Any,
    dim_city: Any,
    dim_merchant_category: Any,
    processed_at: datetime,
) -> Any:
    """Build the SCD2 merchant dimension."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    key_window = Window.orderBy("merchant_dim_id")
    primary_city = _most_common_value(silver_dataframe, "merchant_dim_id", "city", "primary_city")
    primary_category = _most_common_value(
        silver_dataframe,
        "merchant_dim_id",
        "merchant_category",
        "merchant_category",
    )
    metrics = (
        silver_dataframe.groupBy("merchant_dim_id")
        .agg(
            spark_functions.first("merchant_id", ignorenulls=True).alias("merchant_id"),
            spark_functions.min("event_time").alias("first_seen_at"),
            spark_functions.max("event_time").alias("last_seen_at"),
            spark_functions.count("*").cast("long").alias("transaction_count"),
            spark_functions.countDistinct("customer_id").cast("long").alias("distinct_customer_count"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("lifetime_amount"),
            spark_functions.round(spark_functions.avg("amount"), 2)
            .cast("decimal(18,2)")
            .alias("average_transaction_amount"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_transaction_count"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_transaction_count"
            ),
        )
        .join(primary_category, "merchant_dim_id", "left")
        .join(primary_city, "merchant_dim_id", "left")
        .join(dim_merchant_category.select("merchant_category_key", "merchant_category"), "merchant_category", "left")
        .join(dim_city.select(spark_functions.col("city").alias("primary_city"), "city_key"), "primary_city", "left")
        .withColumnRenamed("city_key", "primary_city_key")
        .withColumn("merchant_key", spark_functions.dense_rank().over(key_window).cast("long"))
        .withColumn("fraud_rate", _safe_rate("fraud_transaction_count", "transaction_count"))
        .withColumn("valid_from_ts", spark_functions.col("first_seen_at"))
        .withColumn("valid_to_ts", spark_functions.lit(None).cast("timestamp"))
        .withColumn("is_current", spark_functions.lit(True))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
    )
    return metrics.select(*GOLD_TABLE_COLUMNS["dim_merchant"])


def _build_fact_transactions(
    *,
    silver_dataframe: Any,
    dim_customer: Any,
    dim_account: Any,
    dim_merchant: Any,
    dim_channel: Any,
    dim_city: Any,
    dim_merchant_category: Any,
    processed_at: datetime,
) -> Any:
    """Build the canonical transaction fact table."""

    from pyspark.sql import functions as spark_functions

    return (
        silver_dataframe.join(dim_customer.where("is_current").select("customer_key", "customer_id"), "customer_id")
        .join(dim_account.where("is_current").select("account_key", "account_id"), "account_id")
        .join(dim_merchant.where("is_current").select("merchant_key", "merchant_dim_id"), "merchant_dim_id")
        .join(dim_channel.select("channel_key", "channel"), "channel")
        .join(dim_city.select("city_key", "city"), "city", "left")
        .join(dim_merchant_category.select("merchant_category_key", "merchant_category"), "merchant_category", "left")
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["fact_transactions"])
    )


def _build_fact_transaction_quality_issue(fact_transactions: Any) -> Any:
    """Build the factless quality issue bridge."""

    from pyspark.sql import functions as spark_functions

    return (
        fact_transactions.select(
            "transaction_id",
            spark_functions.posexplode("quality_issue_codes").alias("issue_position_zero", "quality_issue_code"),
            "_gold_processed_at",
        )
        .where(spark_functions.col("quality_issue_code").isNotNull())
        .withColumn("issue_position", (spark_functions.col("issue_position_zero") + spark_functions.lit(1)).cast("int"))
        .select(*GOLD_TABLE_COLUMNS["fact_transaction_quality_issue"])
    )


def _build_fact_customer_daily(fact_transactions: Any) -> Any:
    """Build daily customer aggregate facts."""

    from pyspark.sql import functions as spark_functions

    return (
        fact_transactions.groupBy(
            "customer_key",
            "customer_id",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
        )
        .agg(
            spark_functions.count("*").cast("long").alias("txn_count_1d"),
            _count_when(spark_functions.col("is_approved")).alias("approved_txn_count_1d"),
            _count_when(spark_functions.col("is_declined")).alias("declined_txn_count_1d"),
            _count_when(spark_functions.col("is_reversed")).alias("reversed_txn_count_1d"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("amount_sum_1d"),
            spark_functions.round(spark_functions.avg("amount"), 2).cast("decimal(18,2)").alias("amount_avg_1d"),
            spark_functions.max("amount").cast("decimal(18,2)").alias("amount_max_1d"),
            spark_functions.countDistinct("merchant_key").cast("long").alias("distinct_merchant_count_1d"),
            spark_functions.countDistinct("city_key").cast("long").alias("distinct_city_count_1d"),
            _count_when(spark_functions.col("channel") == spark_functions.lit("online")).alias("online_txn_count_1d"),
            _count_when(spark_functions.col("channel") == spark_functions.lit("card_present")).alias(
                "card_present_txn_count_1d"
            ),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_txn_count_1d"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_txn_count_1d"
            ),
            _count_when(spark_functions.array_contains("quality_issue_codes", "late_arrival")).alias(
                "late_arrival_txn_count_1d"
            ),
            spark_functions.max("_gold_processed_at").alias("_gold_processed_at"),
        )
        .select(*GOLD_TABLE_COLUMNS["fact_customer_daily"])
    )


def _build_fact_account_daily(fact_transactions: Any) -> Any:
    """Build daily account aggregate facts."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    customer_rank_window = Window.partitionBy("account_key", "date_key", "feature_date").orderBy(
        spark_functions.col("_customer_txn_count").desc(),
        spark_functions.col("customer_id"),
        spark_functions.col("customer_key"),
    )
    daily_customer = (
        fact_transactions.groupBy(
            "account_key",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
            "customer_key",
            "customer_id",
        )
        .agg(spark_functions.count("*").cast("long").alias("_customer_txn_count"))
        .withColumn("_customer_rank", spark_functions.row_number().over(customer_rank_window))
        .where(spark_functions.col("_customer_rank") == spark_functions.lit(1))
        .select("account_key", "date_key", "feature_date", "customer_key", "customer_id")
    )
    daily = (
        fact_transactions.groupBy(
            "account_key",
            "account_id",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
        )
        .agg(
            spark_functions.count("*").cast("long").alias("txn_count_1d"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("amount_sum_1d"),
            spark_functions.max("amount").cast("decimal(18,2)").alias("amount_max_1d"),
            spark_functions.countDistinct("merchant_key").cast("long").alias("distinct_merchant_count_1d"),
            spark_functions.countDistinct("city_key").cast("long").alias("distinct_city_count_1d"),
            _count_when(spark_functions.col("is_declined")).alias("declined_txn_count_1d"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_txn_count_1d"),
            spark_functions.max("_gold_processed_at").alias("_gold_processed_at"),
        )
    )
    return daily.join(daily_customer, ["account_key", "date_key", "feature_date"], "left").select(
        *GOLD_TABLE_COLUMNS["fact_account_daily"]
    )


def _build_fact_merchant_daily(fact_transactions: Any) -> Any:
    """Build daily merchant aggregate facts."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    category_rank_window = Window.partitionBy("merchant_key", "date_key", "feature_date").orderBy(
        spark_functions.col("_category_txn_count").desc(),
        spark_functions.col("merchant_category").asc_nulls_last(),
    )
    daily_category = (
        fact_transactions.groupBy(
            "merchant_key",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
            "merchant_category",
        )
        .agg(spark_functions.count("*").cast("long").alias("_category_txn_count"))
        .withColumn("_category_rank", spark_functions.row_number().over(category_rank_window))
        .where(spark_functions.col("_category_rank") == spark_functions.lit(1))
        .select("merchant_key", "date_key", "feature_date", "merchant_category")
    )
    daily = (
        fact_transactions.groupBy(
            "merchant_key",
            "merchant_dim_id",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
        )
        .agg(
            spark_functions.count("*").cast("long").alias("txn_count_1d"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("amount_sum_1d"),
            spark_functions.round(spark_functions.avg("amount"), 2).cast("decimal(18,2)").alias("amount_avg_1d"),
            spark_functions.countDistinct("customer_key").cast("long").alias("distinct_customer_count_1d"),
            _count_when(spark_functions.col("is_declined")).alias("declined_txn_count_1d"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_txn_count_1d"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_txn_count_1d"
            ),
            spark_functions.max("_gold_processed_at").alias("_gold_processed_at"),
        )
        .withColumn("fraud_rate_1d", _safe_rate("fraud_txn_count_1d", "txn_count_1d"))
    )
    return daily.join(daily_category, ["merchant_key", "date_key", "feature_date"], "left").select(
        *GOLD_TABLE_COLUMNS["fact_merchant_daily"]
    )


def _build_fact_city_category_daily(fact_transactions: Any) -> Any:
    """Build daily city and merchant category aggregate facts."""

    from pyspark.sql import functions as spark_functions

    daily = (
        fact_transactions.where(
            spark_functions.col("city_key").isNotNull() & spark_functions.col("merchant_category_key").isNotNull()
        )
        .groupBy(
            "city_key",
            "merchant_category_key",
            "city",
            "merchant_category",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
        )
        .agg(
            spark_functions.count("*").cast("long").alias("txn_count_1d"),
            spark_functions.sum("amount").cast("decimal(18,2)").alias("amount_sum_1d"),
            spark_functions.countDistinct("customer_key").cast("long").alias("distinct_customer_count_1d"),
            spark_functions.countDistinct("merchant_key").cast("long").alias("distinct_merchant_count_1d"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_txn_count_1d"),
            spark_functions.max("_gold_processed_at").alias("_gold_processed_at"),
        )
        .withColumn("fraud_rate_1d", _safe_rate("fraud_txn_count_1d", "txn_count_1d"))
    )
    return daily.select(*GOLD_TABLE_COLUMNS["fact_city_category_daily"])


def _build_fact_device_ip_daily(silver_dataframe: Any, fact_transactions: Any) -> Any:
    """Build daily shared device and IP aggregate facts."""

    from pyspark.sql import functions as spark_functions

    fact_keys = fact_transactions.select(
        "transaction_id",
        "customer_key",
        "account_key",
        "merchant_key",
        "date_key",
        "event_date",
        "is_fraud",
        "quality_status",
        "_gold_processed_at",
    )
    base = silver_dataframe.select("transaction_id", "device_id", "ip_address").join(fact_keys, "transaction_id")
    device_rows = (
        base.where(spark_functions.col("device_id").isNotNull())
        .withColumn("network_identifier", spark_functions.col("device_id"))
        .withColumn("identifier_type", spark_functions.lit("device_id"))
    )
    ip_rows = (
        base.where(spark_functions.col("ip_address").isNotNull())
        .withColumn("network_identifier", spark_functions.col("ip_address"))
        .withColumn("identifier_type", spark_functions.lit("ip_address"))
    )

    return (
        device_rows.unionByName(ip_rows)
        .groupBy(
            "network_identifier",
            "identifier_type",
            "date_key",
            spark_functions.col("event_date").alias("feature_date"),
        )
        .agg(
            spark_functions.count("*").cast("long").alias("txn_count_1d"),
            spark_functions.countDistinct("customer_key").cast("long").alias("distinct_customer_count_1d"),
            spark_functions.countDistinct("account_key").cast("long").alias("distinct_account_count_1d"),
            spark_functions.countDistinct("merchant_key").cast("long").alias("distinct_merchant_count_1d"),
            _count_when(spark_functions.col("is_fraud")).alias("fraud_txn_count_1d"),
            _count_when(spark_functions.col("quality_status") == spark_functions.lit("warning")).alias(
                "warning_txn_count_1d"
            ),
            spark_functions.max("_gold_processed_at").alias("_gold_processed_at"),
        )
        .select(*GOLD_TABLE_COLUMNS["fact_device_ip_daily"])
    )


def _build_feat_customer_rolling(fact_customer_daily: Any, processed_at: datetime) -> Any:
    """Build rolling 7-day and 30-day customer features."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    base = fact_customer_daily.withColumn(
        "_feature_date_epoch",
        spark_functions.unix_timestamp(spark_functions.col("feature_date").cast("timestamp")),
    )
    window_7d = Window.partitionBy("customer_key").orderBy("_feature_date_epoch").rangeBetween(-6 * 86_400, 0)
    window_30d = Window.partitionBy("customer_key").orderBy("_feature_date_epoch").rangeBetween(-29 * 86_400, 0)

    return (
        base.withColumn("event_timestamp", spark_functions.to_timestamp("feature_date"))
        .withColumn("created", spark_functions.lit(processed_at).cast("timestamp"))
        .withColumn("window_start_date", spark_functions.date_sub("feature_date", 29))
        .withColumn("window_end_date", spark_functions.col("feature_date"))
        .withColumn("txn_count_7d", spark_functions.sum("txn_count_1d").over(window_7d).cast("long"))
        .withColumn("txn_count_30d", spark_functions.sum("txn_count_1d").over(window_30d).cast("long"))
        .withColumn("amount_sum_7d", spark_functions.sum("amount_sum_1d").over(window_7d).cast("decimal(18,2)"))
        .withColumn("amount_sum_30d", spark_functions.sum("amount_sum_1d").over(window_30d).cast("decimal(18,2)"))
        .withColumn("amount_avg_7d", _safe_decimal_average("amount_sum_7d", "txn_count_7d"))
        .withColumn("amount_avg_30d", _safe_decimal_average("amount_sum_30d", "txn_count_30d"))
        .withColumn(
            "distinct_merchant_count_7d",
            spark_functions.sum("distinct_merchant_count_1d").over(window_7d).cast("long"),
        )
        .withColumn(
            "distinct_merchant_count_30d",
            spark_functions.sum("distinct_merchant_count_1d").over(window_30d).cast("long"),
        )
        .withColumn("declined_txn_count_7d", spark_functions.sum("declined_txn_count_1d").over(window_7d).cast("long"))
        .withColumn("fraud_txn_count_30d", spark_functions.sum("fraud_txn_count_1d").over(window_30d).cast("long"))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["feat_customer_rolling"])
    )


def _build_feat_customer_total_orders_90d(fact_customer_daily: Any, processed_at: datetime) -> Any:
    """Build the example 90-day customer order-count feature table."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    base = fact_customer_daily.withColumn(
        "_feature_date_epoch",
        spark_functions.unix_timestamp(spark_functions.col("feature_date").cast("timestamp")),
    )
    window_90d = Window.partitionBy("customer_key").orderBy("_feature_date_epoch").rangeBetween(-89 * 86_400, 0)

    return (
        base.withColumn("event_timestamp", spark_functions.to_timestamp("feature_date"))
        .withColumn("created", spark_functions.lit(processed_at).cast("timestamp"))
        .withColumn("total_orders_90d", spark_functions.sum("txn_count_1d").over(window_90d).cast("long"))
        .withColumn(
            "feature_window_start_ts",
            spark_functions.to_timestamp(spark_functions.date_sub("feature_date", 89)),
        )
        .withColumn("feature_window_end_ts", spark_functions.to_timestamp("feature_date"))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["feat_customer_total_orders_90d"])
    )


def _build_feat_transaction_training(
    *,
    fact_transactions: Any,
    feat_customer_rolling: Any,
    fact_account_daily: Any,
    fact_merchant_daily: Any,
    processed_at: datetime,
) -> Any:
    """Build transaction-level training rows with point-in-time feature joins."""

    from pyspark.sql import functions as spark_functions

    transaction_base = fact_transactions.withColumn("previous_feature_date", spark_functions.date_sub("event_date", 1))
    customer_features = feat_customer_rolling.select(
        "customer_key",
        spark_functions.col("feature_date").alias("previous_feature_date"),
        spark_functions.col("txn_count_7d").alias("customer_txn_count_7d"),
        spark_functions.col("amount_sum_30d").alias("customer_amount_sum_30d"),
        spark_functions.col("distinct_merchant_count_7d").alias("customer_distinct_merchant_count_7d"),
    )
    account_features = fact_account_daily.select(
        "account_key",
        spark_functions.col("feature_date").alias("previous_feature_date"),
        spark_functions.col("txn_count_1d").alias("account_txn_count_1d"),
        spark_functions.col("amount_sum_1d").alias("account_amount_sum_1d"),
    )
    merchant_features = fact_merchant_daily.select(
        "merchant_key",
        spark_functions.col("feature_date").alias("previous_feature_date"),
        spark_functions.col("txn_count_1d").alias("merchant_txn_count_1d"),
        spark_functions.col("fraud_rate_1d").alias("merchant_fraud_rate_1d"),
    )

    return (
        transaction_base.join(customer_features, ["customer_key", "previous_feature_date"], "left")
        .join(account_features, ["account_key", "previous_feature_date"], "left")
        .join(merchant_features, ["merchant_key", "previous_feature_date"], "left")
        .withColumn("event_timestamp", spark_functions.col("event_time"))
        .withColumn("created", spark_functions.lit(processed_at).cast("timestamp"))
        .withColumn("device_distinct_customer_count_1d", spark_functions.lit(None).cast("long"))
        .withColumn("ip_distinct_account_count_1d", spark_functions.lit(None).cast("long"))
        .withColumn("_gold_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
        .select(*GOLD_TABLE_COLUMNS["feat_transaction_training"])
    )


def _most_common_value(dataframe: Any, group_column: str, value_column: str, output_column: str) -> Any:
    """Return the most common non-null value per group."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import Window

    rank_window = Window.partitionBy(group_column).orderBy(
        spark_functions.col("value_count").desc(),
        spark_functions.col(value_column),
    )
    return (
        dataframe.where(spark_functions.col(value_column).isNotNull())
        .groupBy(group_column, value_column)
        .agg(spark_functions.count("*").alias("value_count"))
        .withColumn("_value_rank", spark_functions.row_number().over(rank_window))
        .where(spark_functions.col("_value_rank") == spark_functions.lit(1))
        .select(group_column, spark_functions.col(value_column).alias(output_column))
    )


def _count_when(condition: Any) -> Any:
    """Count rows matching a Spark boolean condition."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.sum(
        spark_functions.when(condition, spark_functions.lit(1)).otherwise(spark_functions.lit(0))
    ).cast("long")


def _safe_rate(numerator_column: str, denominator_column: str) -> Any:
    """Return a Spark expression for a safe double ratio."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.when(
        spark_functions.col(denominator_column) > spark_functions.lit(0),
        spark_functions.col(numerator_column).cast("double") / spark_functions.col(denominator_column).cast("double"),
    ).otherwise(spark_functions.lit(0.0))


def _safe_decimal_average(amount_column: str, count_column: str) -> Any:
    """Return a decimal average from amount and count columns."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.when(
        spark_functions.col(count_column) > spark_functions.lit(0),
        spark_functions.round(spark_functions.col(amount_column) / spark_functions.col(count_column), 2),
    ).otherwise(spark_functions.lit(0)).cast("decimal(18,2)")


def _write_gold_tables(frames: GoldBuildFrames, config: GoldTransactionsConfig) -> list[GoldTableResult]:
    """Write all Gold tables to their documented Parquet paths."""

    table_writes = (
        ("dim_date", frames.dim_date, ()),
        ("dim_city", frames.dim_city, ()),
        ("dim_channel", frames.dim_channel, ()),
        ("dim_quality_issue", frames.dim_quality_issue, ()),
        ("dim_merchant_category", frames.dim_merchant_category, ()),
        ("dim_customer", frames.dim_customer, ()),
        ("dim_account", frames.dim_account, ()),
        ("dim_merchant", frames.dim_merchant, ()),
        ("fact_transactions", frames.fact_transactions, ("event_date",)),
        ("fact_transaction_quality_issue", frames.fact_transaction_quality_issue, ()),
        ("fact_customer_daily", frames.fact_customer_daily, ("feature_date",)),
        ("fact_account_daily", frames.fact_account_daily, ("feature_date",)),
        ("fact_merchant_daily", frames.fact_merchant_daily, ("feature_date",)),
        ("fact_city_category_daily", frames.fact_city_category_daily, ("feature_date",)),
        ("fact_device_ip_daily", frames.fact_device_ip_daily, ("feature_date",)),
        ("feat_customer_rolling", frames.feat_customer_rolling, ("feature_date",)),
        ("feat_customer_total_orders_90d", frames.feat_customer_total_orders_90d, ()),
        ("feat_transaction_training", frames.feat_transaction_training, ()),
    )

    results: list[GoldTableResult] = []
    for table_name, dataframe, partition_columns in table_writes:
        output_path = config.output_dir / table_name
        selected_dataframe = _select_gold_columns(dataframe, table_name)
        row_count = selected_dataframe.count()
        writer = selected_dataframe.write.mode(config.write_mode)
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(str(output_path))
        results.append(GoldTableResult(table_name=table_name, output_path=output_path, row_count=row_count))
    return results


def _select_gold_columns(dataframe: Any, table_name: str) -> Any:
    """Select documented Gold columns and fail if any are missing."""

    columns = GOLD_TABLE_COLUMNS[table_name]
    missing_columns = [column for column in columns if column not in dataframe.columns]
    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(f"{table_name} is missing required columns: {missing_text}")
    return dataframe.select(*columns)


def _write_summary(result: GoldTransactionsResult, output_dir: Path) -> None:
    """Write the Gold build summary JSON."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def _parse_datetime(raw_value: str) -> datetime:
    """Parse an ISO timestamp for deterministic local runs."""

    value = raw_value.replace("Z", "+00:00")
    parsed_value = datetime.fromisoformat(value)
    if parsed_value.tzinfo is None:
        return parsed_value.replace(tzinfo=UTC)
    return parsed_value.astimezone(UTC)


def _to_utc_string(value: datetime) -> str:
    """Format a datetime as a stable UTC ISO string."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for Gold transactions."""

    parser = argparse.ArgumentParser(description="Build Gold transaction Parquet tables from Silver.")
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=sorted(SUPPORTED_WRITE_MODES), default=DEFAULT_WRITE_MODE)
    parser.add_argument("--processed-at", help="Optional ISO timestamp used for _gold_processed_at.")
    return parser

def main(argv: list[str] | None = None) -> int:
    """Run the Gold transaction build from the command line."""

    args = build_parser().parse_args(argv)
    config = GoldTransactionsConfig(
        silver_dir=args.silver_dir,
        output_dir=args.output_dir,
        master=args.master,
        write_mode=args.write_mode,
        processed_at=_parse_datetime(args.processed_at) if args.processed_at else None,
    )
    try:
        result = build_gold_transactions(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())