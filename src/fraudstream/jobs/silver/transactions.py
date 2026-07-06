"""Build the Silver transaction table from Bronze transaction Parquet.

Silver is the first layer that changes business values. This job standardizes
raw strings, casts analytical types, assigns quality issue codes, deduplicates
by transaction_id with deterministic tie-breaking, and writes clean Parquet for
downstream feature and analytics work.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path
from typing import Any, Iterable, Sequence

from fraudstream.jobs.bronze.ingest_transactions import (
    DEFAULT_MASTER,
    DEFAULT_OUTPUT_DIR as DEFAULT_BRONZE_DIR,
    SCHEMA_VERSION_V2,
    SUPPORTED_WRITE_MODES,
)


APP_NAME = "FraudStreamSilverTransactions"
DEFAULT_OUTPUT_DIR = Path("data/silver/transactions")
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_silver_transactions_summary.json"
LATE_ARRIVAL_THRESHOLD_MINUTES = 60.0
EXPECTED_CURRENCY = "USD"
EXPECTED_CHANNELS = ("atm", "card_present", "mobile_wallet", "online")
EXPECTED_TRANSACTION_STATUSES = ("approved", "declined", "reversed")
QUALITY_STATUS_VALID = "valid"
QUALITY_STATUS_WARNING = "warning"
QUALITY_STATUS_QUARANTINED = "quarantined"

SILVER_COLUMNS = [
    "transaction_id",
    "account_id",
    "customer_id",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "city",
    "channel",
    "transaction_status",
    "is_fraud",
    "event_time",
    "source_created_at",
    "arrival_delay_minutes",
    "device_id",
    "ip_address",
    "authentication_method",
    "risk_signal_version",
    "quality_status",
    "quality_issue_codes",
    "duplicate_record_count",
    "dedup_rank",
    "_bronze_ingest_run_id",
    "_bronze_source_file_path",
    "_bronze_source_row_number",
    "_bronze_raw_record_hash",
    "_silver_processed_at",
    "event_date",
]


@dataclass(frozen=True)
class SilverTransactionsConfig:
    """Runtime settings for the Silver transaction build."""

    bronze_dir: Path = DEFAULT_BRONZE_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    processed_at: datetime | None = None

    def validate(self) -> None:
        """Raise when the Silver job cannot run with this config."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        if not self.bronze_dir.exists():
            raise FileNotFoundError(f"bronze_dir does not exist: {self.bronze_dir}")


@dataclass(frozen=True)
class SilverTransactionsResult:
    """Summary of one Silver transaction build."""

    bronze_dir: Path
    output_dir: Path
    input_row_count: int
    output_row_count: int
    quarantined_row_count: int
    warning_row_count: int
    valid_row_count: int
    duplicate_transaction_id_count: int
    duplicate_rows_removed_count: int
    event_date_count: int
    write_mode: str
    spark_version: str
    processed_at: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable job summary."""

        return {
            "bronze_dir": str(self.bronze_dir),
            "output_dir": str(self.output_dir),
            "input_row_count": self.input_row_count,
            "output_row_count": self.output_row_count,
            "quarantined_row_count": self.quarantined_row_count,
            "warning_row_count": self.warning_row_count,
            "valid_row_count": self.valid_row_count,
            "duplicate_transaction_id_count": self.duplicate_transaction_id_count,
            "duplicate_rows_removed_count": self.duplicate_rows_removed_count,
            "event_date_count": self.event_date_count,
            "write_mode": self.write_mode,
            "spark_version": self.spark_version,
            "processed_at": self.processed_at,
            "completed_at": self.completed_at,
        }


@dataclass(frozen=True)
class SilverTransactionsMetrics:
    """Row counts collected from the ranked Silver candidate DataFrame."""

    input_row_count: int
    output_row_count: int
    quarantined_row_count: int
    warning_row_count: int
    valid_row_count: int
    duplicate_transaction_id_count: int
    duplicate_rows_removed_count: int
    event_date_count: int


def build_silver_transactions(config: SilverTransactionsConfig) -> SilverTransactionsResult:
    """Read Bronze transactions, deduplicate, and write Silver Parquet."""

    config.validate()
    spark = _build_spark_session(config.master)
    ranked_dataframe = None
    try:
        processed_at = config.processed_at or datetime.now(UTC)
        bronze_dataframe = spark.read.parquet(str(config.bronze_dir))
        cleaned_dataframe = _clean_bronze_transactions(bronze_dataframe, processed_at)
        ranked_dataframe = _persist_for_reuse(_rank_transactions_for_deduplication(cleaned_dataframe))
        silver_dataframe = _select_silver_rows(ranked_dataframe)

        _write_silver_parquet(silver_dataframe, config)

        result = _build_result(
            ranked_dataframe=ranked_dataframe,
            config=config,
            processed_at=processed_at,
            spark_version=spark.version,
        )
        _write_summary(result, config.output_dir)
        return result
    finally:
        if ranked_dataframe is not None:
            ranked_dataframe.unpersist(blocking=False)
        spark.stop()


def _build_spark_session(master: str) -> Any:
    """Create a Spark session or raise a clear dependency error."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark`, then retry this command."
        ) from exc

    return (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _clean_bronze_transactions(bronze_dataframe: Any, processed_at: datetime) -> Any:
    """Standardize raw Bronze values and add quality metadata."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql import types as spark_types

    amount_text = spark_functions.regexp_replace(
        spark_functions.trim(spark_functions.col("amount")),
        ",",
        "",
    )
    
    amount_decimal = (
        spark_functions.when(amount_text.rlike(r"^-?\d+(\.\d+)?$"), amount_text)
        .otherwise(spark_functions.lit(None))
        .cast(spark_types.DecimalType(18, 2))
    )

    event_time = spark_functions.to_timestamp(_trimmed_or_null("event_timestamp"))
    source_created_at = spark_functions.to_timestamp(_trimmed_or_null("created_ts"))
    arrival_delay_minutes = (
        (spark_functions.unix_timestamp(source_created_at) - spark_functions.unix_timestamp(event_time)) / 60.0
    )

    standardized_dataframe = (
        bronze_dataframe.withColumn("transaction_id", _trimmed_required("transaction_id"))
        .withColumn("account_id", _trimmed_required("account_id"))
        .withColumn("customer_id", _trimmed_required("customer_id"))
        .withColumn("merchant_id", _trimmed_or_null("merchant_id"))
        .withColumn("merchant_category", spark_functions.lower(_trimmed_or_null("merchant_category")))
        .withColumn("amount", amount_decimal)
        .withColumn("currency", spark_functions.upper(_trimmed_required("currency")))
        .withColumn("city", _title_case_or_null(_trimmed_or_null("city")))
        .withColumn("channel", spark_functions.lower(_trimmed_required("channel")))
        .withColumn("transaction_status", spark_functions.lower(_trimmed_required("transaction_status")))
        .withColumn("is_fraud", _cast_fraud_label("is_fraud"))
        .withColumn("event_time", event_time)
        .withColumn("source_created_at", source_created_at)
        .withColumn("arrival_delay_minutes", arrival_delay_minutes.cast("double"))
        .withColumn("device_id", _trimmed_or_null("device_id"))
        .withColumn("ip_address", _trimmed_or_null("ip_address"))
        .withColumn("authentication_method", spark_functions.lower(_trimmed_or_null("authentication_method")))
        .withColumn("risk_signal_version", _trimmed_or_null("risk_signal_version"))
        .withColumn("event_date", spark_functions.to_date(spark_functions.col("event_time")))
        .withColumn("_bronze_ingest_run_id", spark_functions.col("_ingest_run_id"))
        .withColumn("_bronze_source_file_path", spark_functions.col("_source_file_path"))
        .withColumn("_bronze_source_row_number", spark_functions.col("_source_row_number").cast("long"))
        .withColumn("_bronze_raw_record_hash", spark_functions.col("_raw_record_hash"))
        .withColumn("_silver_processed_at", spark_functions.lit(processed_at).cast("timestamp"))
    )

    return _add_quality_columns(standardized_dataframe)


def _trimmed_required(column_name: str) -> Any:
    """Return a trimmed string, using an empty string when the source is null."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.trim(
        spark_functions.coalesce(spark_functions.col(column_name), spark_functions.lit(""))
    )


def _trimmed_or_null(column_name: str) -> Any:
    """Return a trimmed string, converting blanks and nulls to null."""

    from pyspark.sql import functions as spark_functions

    trimmed_value = _trimmed_required(column_name)
    return spark_functions.when(trimmed_value == "", spark_functions.lit(None).cast("string")).otherwise(
        trimmed_value
    )


def _title_case_or_null(column: Any) -> Any:
    """Normalize a nullable city-like string to title case."""

    from pyspark.sql import functions as spark_functions

    collapsed_value = spark_functions.regexp_replace(column, r"\s+", " ")
    return spark_functions.when(column.isNull(), spark_functions.lit(None).cast("string")).otherwise(
        spark_functions.initcap(collapsed_value)
    )


def _cast_fraud_label(column_name: str) -> Any:
    """Cast the raw fraud label into a boolean."""

    from pyspark.sql import functions as spark_functions

    label = _trimmed_required(column_name)
    return (
        spark_functions.when(label == "1", spark_functions.lit(True))
        .when(label == "0", spark_functions.lit(False))
        .otherwise(spark_functions.lit(None).cast("boolean"))
    )


def _add_quality_columns(dataframe: Any) -> Any:
    """Add quality issue codes and a row-level quality status."""

    from pyspark.sql import functions as spark_functions

    quarantine_conditions = [
        (spark_functions.col("transaction_id") == "", "missing_transaction_id"),
        (spark_functions.col("account_id") == "", "missing_account_id"),
        (spark_functions.col("customer_id") == "", "missing_customer_id"),
        (spark_functions.col("amount").isNull(), "invalid_amount"),
        (spark_functions.col("amount") < 0, "negative_amount"),
        (spark_functions.col("event_time").isNull(), "invalid_event_time"),
        (spark_functions.col("is_fraud").isNull(), "invalid_fraud_label"),
    ]
    warning_conditions = [
        (spark_functions.col("currency") != EXPECTED_CURRENCY, "unexpected_currency"),
        (~spark_functions.col("channel").isin(*EXPECTED_CHANNELS), "unexpected_channel"),
        (
            ~spark_functions.col("transaction_status").isin(*EXPECTED_TRANSACTION_STATUSES),
            "unexpected_status",
        ),
        (spark_functions.col("arrival_delay_minutes") < 0, "negative_arrival_delay"),
        (spark_functions.col("arrival_delay_minutes") > LATE_ARRIVAL_THRESHOLD_MINUTES, "late_arrival"),
        (
            (spark_functions.col("schema_version") == SCHEMA_VERSION_V2)
            & (spark_functions.col("device_id").isNull() | spark_functions.col("ip_address").isNull()),
            "missing_evolved_value",
        ),
    ]

    issue_codes = _issue_code_array([*quarantine_conditions, *warning_conditions])
    quarantine_condition = _any_condition(condition for condition, _ in quarantine_conditions)

    return (
        dataframe.withColumn("quality_issue_codes", issue_codes)
        .withColumn(
            "quality_status",
            spark_functions.when(quarantine_condition, spark_functions.lit(QUALITY_STATUS_QUARANTINED))
            .when(
                spark_functions.size(spark_functions.col("quality_issue_codes")) > 0,
                spark_functions.lit(QUALITY_STATUS_WARNING),
            )
            .otherwise(spark_functions.lit(QUALITY_STATUS_VALID)),
        )
    )


def _issue_code_array(conditions: Sequence[tuple[Any, str]]) -> Any:
    """Build an array of quality issue codes from boolean conditions."""

    from pyspark.sql import functions as spark_functions

    issue_expressions = [
        spark_functions.when(condition, spark_functions.lit(code)).otherwise(
            spark_functions.lit(None).cast("string")
        )
        for condition, code in conditions
    ]
    return spark_functions.filter(spark_functions.array(*issue_expressions), lambda item: item.isNotNull())


def _any_condition(conditions: Iterable[Any]) -> Any:
    """Combine Spark boolean columns with OR."""

    from pyspark.sql import functions as spark_functions

    return reduce(lambda left, right: left | right, conditions, spark_functions.lit(False))


def _rank_transactions_for_deduplication(dataframe: Any) -> Any:
    """Assign deterministic row ranks inside each transaction_id group."""

    from pyspark.sql import Window
    from pyspark.sql import functions as spark_functions

    transaction_window = Window.partitionBy("transaction_id")
    ranking_window = transaction_window.orderBy(
        spark_functions.when(spark_functions.col("quality_status") == QUALITY_STATUS_QUARANTINED, 1)
        .otherwise(0)
        .asc(),
        spark_functions.when(_core_fields_parseable(), 0).otherwise(1).asc(),
        spark_functions.col("source_created_at").desc_nulls_last(),
        spark_functions.col("_ingested_at").desc_nulls_last(),
        spark_functions.col("_bronze_source_row_number").desc_nulls_last(),
        spark_functions.col("_bronze_raw_record_hash").asc_nulls_last(),
        spark_functions.col("_bronze_source_file_path").asc_nulls_last(),
    )

    return (
        dataframe.withColumn(
            "duplicate_record_count",
            spark_functions.count("*").over(transaction_window).cast("int"),
        )
        .withColumn("dedup_rank", spark_functions.row_number().over(ranking_window).cast("int"))
    )


def _core_fields_parseable() -> Any:
    """Return the core parseability condition used during dedup ranking."""

    from pyspark.sql import functions as spark_functions

    return (
        spark_functions.col("event_time").isNotNull()
        & spark_functions.col("amount").isNotNull()
        & spark_functions.col("is_fraud").isNotNull()
    )


def _select_silver_rows(ranked_dataframe: Any) -> Any:
    """Return selected non-quarantined rows in stable Silver column order."""

    return ranked_dataframe.where(_is_selected_silver_row()).select(*SILVER_COLUMNS)


def _is_selected_silver_row() -> Any:
    """Return the predicate for rows written to Silver."""

    from pyspark.sql import functions as spark_functions

    return (spark_functions.col("dedup_rank") == 1) & (
        spark_functions.col("quality_status") != QUALITY_STATUS_QUARANTINED
    )


def _persist_for_reuse(dataframe: Any) -> Any:
    """Persist ranked candidates because writing and metrics reuse them."""

    from pyspark import StorageLevel

    return dataframe.persist(StorageLevel.MEMORY_AND_DISK)


def _write_silver_parquet(silver_dataframe: Any, config: SilverTransactionsConfig) -> None:
    """Write selected Silver rows as partitioned Parquet."""

    (
        silver_dataframe.write.mode(config.write_mode)
        .partitionBy("event_date")
        .parquet(str(config.output_dir))
    )


def _build_result(
    ranked_dataframe: Any,
    config: SilverTransactionsConfig,
    processed_at: datetime,
    spark_version: str,
) -> SilverTransactionsResult:
    """Build a compact summary for the completed Silver job."""

    metrics = _collect_metrics(ranked_dataframe)

    return SilverTransactionsResult(
        bronze_dir=config.bronze_dir,
        output_dir=config.output_dir,
        input_row_count=metrics.input_row_count,
        output_row_count=metrics.output_row_count,
        quarantined_row_count=metrics.quarantined_row_count,
        warning_row_count=metrics.warning_row_count,
        valid_row_count=metrics.valid_row_count,
        duplicate_transaction_id_count=metrics.duplicate_transaction_id_count,
        duplicate_rows_removed_count=metrics.duplicate_rows_removed_count,
        event_date_count=metrics.event_date_count,
        write_mode=config.write_mode,
        spark_version=spark_version,
        processed_at=_to_utc_string(processed_at),
        completed_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    )


def _collect_metrics(ranked_dataframe: Any) -> SilverTransactionsMetrics:
    """Collect Silver evidence metrics in one Spark aggregation."""

    from pyspark.sql import functions as spark_functions

    selected_row = _is_selected_silver_row()
    valid_selected_row = selected_row & (spark_functions.col("quality_status") == QUALITY_STATUS_VALID)
    warning_selected_row = selected_row & (spark_functions.col("quality_status") == QUALITY_STATUS_WARNING)
    quarantined_row = spark_functions.col("quality_status") == QUALITY_STATUS_QUARANTINED
    duplicate_group = (spark_functions.col("dedup_rank") == 1) & (
        spark_functions.col("duplicate_record_count") > 1
    )
    removable_duplicate_row = (spark_functions.col("dedup_rank") > 1) & (
        spark_functions.col("quality_status") != QUALITY_STATUS_QUARANTINED
    )

    metrics_row = ranked_dataframe.agg(
        spark_functions.count("*").alias("input_row_count"),
        _count_when(selected_row).alias("output_row_count"),
        _count_when(quarantined_row).alias("quarantined_row_count"),
        _count_when(warning_selected_row).alias("warning_row_count"),
        _count_when(valid_selected_row).alias("valid_row_count"),
        _count_when(duplicate_group).alias("duplicate_transaction_id_count"),
        _count_when(removable_duplicate_row).alias("duplicate_rows_removed_count"),
        spark_functions.countDistinct(spark_functions.when(selected_row, spark_functions.col("event_date"))).alias(
            "event_date_count"
        ),
    ).first()

    return SilverTransactionsMetrics(
        input_row_count=_metric_value(metrics_row, "input_row_count"),
        output_row_count=_metric_value(metrics_row, "output_row_count"),
        quarantined_row_count=_metric_value(metrics_row, "quarantined_row_count"),
        warning_row_count=_metric_value(metrics_row, "warning_row_count"),
        valid_row_count=_metric_value(metrics_row, "valid_row_count"),
        duplicate_transaction_id_count=_metric_value(metrics_row, "duplicate_transaction_id_count"),
        duplicate_rows_removed_count=_metric_value(metrics_row, "duplicate_rows_removed_count"),
        event_date_count=_metric_value(metrics_row, "event_date_count"),
    )


def _count_when(condition: Any) -> Any:
    """Return an aggregate expression that counts rows matching a condition."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.coalesce(
        spark_functions.sum(spark_functions.when(condition, 1).otherwise(0)),
        spark_functions.lit(0),
    ).cast("long")


def _metric_value(metrics_row: Any, column_name: str) -> int:
    """Return one metric from a Spark Row as a plain Python int."""

    return int(metrics_row[column_name] or 0)


def _write_summary(result: SilverTransactionsResult, output_dir: Path) -> None:
    """Write a JSON evidence summary next to the Silver Parquet partitions."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def _parse_datetime(raw_value: str) -> datetime:
    """Parse an ISO timestamp for deterministic local test runs."""

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
    """Build the command-line parser for Silver transactions."""

    parser = argparse.ArgumentParser(
        description="Build deduplicated Silver transaction Parquet from Bronze."
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=sorted(SUPPORTED_WRITE_MODES), default=DEFAULT_WRITE_MODE)
    parser.add_argument("--processed-at", help="Optional ISO timestamp used for _silver_processed_at.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Silver transaction deduplication from the command line."""

    args = build_parser().parse_args(argv)
    config = SilverTransactionsConfig(
        bronze_dir=args.bronze_dir,
        output_dir=args.output_dir,
        master=args.master,
        write_mode=args.write_mode,
        processed_at=_parse_datetime(args.processed_at) if args.processed_at else None,
    )
    try:
        result = build_silver_transactions(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
