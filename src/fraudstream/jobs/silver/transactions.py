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
from dataclasses import dataclass, field
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
from fraudstream.jobs.spark_ui import (
    SparkUIConfig,
    add_spark_ui_arguments,
    announce_spark_ui,
    clear_spark_job_group,
    configure_spark_builder,
    retain_spark_ui,
    set_spark_job_group,
    spark_ui_config_from_args,
)


APP_NAME = "FraudStreamSilverTransactions"
DEFAULT_OUTPUT_DIR = Path("data/silver/transactions")
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_silver_transactions_summary.json"
QUALITY_REPORT_FILE_NAME = "_silver_quality_report.json"
QUALITY_OUTPUT_DIR_NAME = "transaction_quality_issues"
LATE_ARRIVAL_THRESHOLD_MINUTES = 60.0
EXPECTED_CURRENCY = "USD"
EXPECTED_CHANNELS = ("atm", "card_present", "mobile_wallet", "online")
EXPECTED_TRANSACTION_STATUSES = ("approved", "declined", "reversed")
QUALITY_STATUS_VALID = "valid"
QUALITY_STATUS_WARNING = "warning"
QUALITY_STATUS_QUARANTINED = "quarantined"
RECORD_ACTION_SELECTED = "selected"
RECORD_ACTION_DUPLICATE_REJECTED = "duplicate_rejected"
RECORD_ACTION_QUARANTINED = "quarantined"

REQUIRED_TRANSACTION_KEY_RULES = [
    {
        "field": "transaction_id",
        "rule": "must be non-null and non-blank",
        "quality_code": "missing_transaction_id",
        "behavior": "quarantine and write to quality evidence output",
    },
    {
        "field": "account_id",
        "rule": "must be non-null and non-blank",
        "quality_code": "missing_account_id",
        "behavior": "quarantine and write to quality evidence output",
    },
    {
        "field": "customer_id",
        "rule": "must be non-null and non-blank",
        "quality_code": "missing_customer_id",
        "behavior": "quarantine and write to quality evidence output",
    },
]
NULLABLE_FIELD_RULES = [
    {"field": "merchant_id", "behavior": "blank becomes null; row remains usable"},
    {"field": "merchant_category", "behavior": "blank becomes null; row remains usable"},
    {"field": "city", "behavior": "blank becomes null; row remains usable"},
    {"field": "source_created_at", "behavior": "unparseable created_ts becomes null; row remains usable"},
    {"field": "device_id", "behavior": "blank becomes null; v2 missing values are warnings"},
    {"field": "ip_address", "behavior": "blank becomes null; v2 missing values are warnings"},
    {"field": "authentication_method", "behavior": "blank becomes null; row remains usable"},
    {"field": "risk_signal_version", "behavior": "blank becomes null; row remains usable"},
]

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

QUALITY_ISSUE_COLUMNS = [
    *SILVER_COLUMNS,
    "_silver_record_action",
    "_silver_quality_reported_at",
]


@dataclass(frozen=True)
class SilverTransactionsConfig:
    """Runtime settings for the Silver transaction build."""

    bronze_dir: Path = DEFAULT_BRONZE_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    quality_output_dir: Path | None = None
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    processed_at: datetime | None = None
    spark_ui: SparkUIConfig = field(default_factory=SparkUIConfig)

    def validate(self) -> None:
        """Raise when the Silver job cannot run with this config."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        if not self.bronze_dir.exists():
            raise FileNotFoundError(f"bronze_dir does not exist: {self.bronze_dir}")
        self.spark_ui.validate()

    @property
    def resolved_quality_output_dir(self) -> Path:
        """Return the output path for quarantined and warning evidence rows."""

        return self.quality_output_dir or self.output_dir.parent / QUALITY_OUTPUT_DIR_NAME


@dataclass(frozen=True)
class SilverTransactionsResult:
    """Summary of one Silver transaction build."""

    bronze_dir: Path
    output_dir: Path
    quality_output_dir: Path
    quality_report_path: Path
    input_row_count: int
    output_row_count: int
    quality_issue_row_count: int
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
            "quality_output_dir": str(self.quality_output_dir),
            "quality_report_path": str(self.quality_report_path),
            "input_row_count": self.input_row_count,
            "output_row_count": self.output_row_count,
            "quality_issue_row_count": self.quality_issue_row_count,
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
    quality_issue_row_count: int
    quarantined_row_count: int
    warning_row_count: int
    valid_row_count: int
    duplicate_transaction_id_count: int
    duplicate_rows_removed_count: int
    event_date_count: int


def build_silver_transactions(config: SilverTransactionsConfig) -> SilverTransactionsResult:
    """Read Bronze transactions, deduplicate, and write Silver Parquet."""

    config.validate()
    spark = _build_spark_session(config.master, config.spark_ui)
    ranked_dataframe = None
    try:
        announce_spark_ui(spark, config.spark_ui)
        processed_at = config.processed_at or datetime.now(UTC)
        bronze_dataframe = spark.read.parquet(str(config.bronze_dir))
        cleaned_dataframe = _clean_bronze_transactions(bronze_dataframe, processed_at)
        ranked_dataframe = _persist_for_reuse(_rank_transactions_for_deduplication(cleaned_dataframe))
        silver_dataframe = _select_silver_rows(ranked_dataframe)
        quality_issue_dataframe = _select_quality_issue_rows(ranked_dataframe, processed_at)

        set_spark_job_group(
            spark,
            "silver-write-selected-transactions",
            "Silver: clean types, detect late arrivals, rank duplicates, and write selected rows",
        )
        _write_silver_parquet(silver_dataframe, config)
        set_spark_job_group(
            spark,
            "silver-write-quality-evidence",
            "Silver: write quarantined, warning, and duplicate-rejected evidence",
        )
        _write_quality_issue_parquet(quality_issue_dataframe, config)

        set_spark_job_group(
            spark,
            "silver-profile-offline-problems",
            "Silver: measure duplicate, late-arrival, schema-evolution, and quality outcomes",
        )
        result = _build_result(
            ranked_dataframe=ranked_dataframe,
            config=config,
            processed_at=processed_at,
            spark_version=spark.version,
        )
        _write_summary(result, config.output_dir)
        _write_quality_report(_build_quality_report(ranked_dataframe, result), result.quality_report_path)
        clear_spark_job_group(spark)
        retain_spark_ui(spark, config.spark_ui)
        return result
    finally:
        if ranked_dataframe is not None:
            ranked_dataframe.unpersist(blocking=False)
        spark.stop()


def _build_spark_session(master: str, spark_ui: SparkUIConfig | None = None) -> Any:
    """Create a Spark session or raise a clear dependency error."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark`, then retry this command."
        ) from exc

    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return configure_spark_builder(builder, spark_ui or SparkUIConfig()).getOrCreate()


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
        .withColumn("merchant_category", _normalized_lower_code_or_null("merchant_category"))
        .withColumn("amount", amount_decimal)
        .withColumn("currency", _normalized_upper_code_required("currency"))
        .withColumn("city", _normalized_city_or_null("city"))
        .withColumn("channel", _normalized_lower_code_required("channel"))
        .withColumn("transaction_status", _normalized_lower_code_required("transaction_status"))
        .withColumn("is_fraud", _cast_fraud_label("is_fraud"))
        .withColumn("event_time", event_time)
        .withColumn("source_created_at", source_created_at)
        .withColumn("arrival_delay_minutes", arrival_delay_minutes.cast("double"))
        .withColumn("device_id", _trimmed_or_null("device_id"))
        .withColumn("ip_address", _trimmed_or_null("ip_address"))
        .withColumn("authentication_method", _normalized_lower_code_or_null("authentication_method"))
        .withColumn("risk_signal_version", _normalized_lower_code_or_null("risk_signal_version"))
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
    title_cased_value = spark_functions.initcap(spark_functions.lower(collapsed_value))
    return spark_functions.when(column.isNull(), spark_functions.lit(None).cast("string")).otherwise(
        title_cased_value
    )


def _normalized_city_or_null(column_name: str) -> Any:
    """Return a display-safe city string or null for blank values."""

    return _title_case_or_null(_trimmed_or_null(column_name))


def _normalized_lower_code_required(column_name: str) -> Any:
    """Return a required code-like string in lowercase snake case."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.lower(_normalize_code_separators(_trimmed_required(column_name)))


def _normalized_lower_code_or_null(column_name: str) -> Any:
    """Return an optional code-like string in lowercase snake case."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.lower(_normalize_code_separators(_trimmed_or_null(column_name)))


def _normalized_upper_code_required(column_name: str) -> Any:
    """Return a required code-like string in uppercase snake case."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.upper(_normalize_code_separators(_trimmed_required(column_name)))


def _normalize_code_separators(column: Any) -> Any:
    """Convert spaces and hyphens in enum-like strings to single underscores."""

    from pyspark.sql import functions as spark_functions

    normalized_value = spark_functions.regexp_replace(column, r"[\s-]+", "_")
    return spark_functions.regexp_replace(normalized_value, r"_+", "_")


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


def _select_quality_issue_rows(ranked_dataframe: Any, processed_at: datetime) -> Any:
    """Return warning, quarantined, and duplicate-rejected rows for audit."""

    from pyspark.sql import functions as spark_functions

    return (
        ranked_dataframe.withColumn("_silver_record_action", _record_action())
        .withColumn("_silver_quality_reported_at", spark_functions.lit(processed_at).cast("timestamp"))
        .where(_is_quality_evidence_row())
        .select(*QUALITY_ISSUE_COLUMNS)
    )


def _is_selected_silver_row() -> Any:
    """Return the predicate for rows written to Silver."""

    from pyspark.sql import functions as spark_functions

    return (spark_functions.col("dedup_rank") == 1) & (
        spark_functions.col("quality_status") != QUALITY_STATUS_QUARANTINED
    )


def _is_quality_evidence_row() -> Any:
    """Return true when a row needs quality or rejection evidence."""

    from pyspark.sql import functions as spark_functions

    return (spark_functions.size(spark_functions.col("quality_issue_codes")) > 0) | (
        _record_action() != spark_functions.lit(RECORD_ACTION_SELECTED)
    )


def _record_action() -> Any:
    """Classify how a ranked row is handled by the Silver build."""

    from pyspark.sql import functions as spark_functions

    return (
        spark_functions.when(
            spark_functions.col("quality_status") == QUALITY_STATUS_QUARANTINED,
            spark_functions.lit(RECORD_ACTION_QUARANTINED),
        )
        .when(
            (spark_functions.col("dedup_rank") > 1)
            & (spark_functions.col("quality_status") != QUALITY_STATUS_QUARANTINED),
            spark_functions.lit(RECORD_ACTION_DUPLICATE_REJECTED),
        )
        .otherwise(spark_functions.lit(RECORD_ACTION_SELECTED))
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


def _write_quality_issue_parquet(quality_issue_dataframe: Any, config: SilverTransactionsConfig) -> None:
    """Write rows needing quality evidence to a separate Parquet table."""

    (
        quality_issue_dataframe.write.mode(config.write_mode)
        .partitionBy("quality_status")
        .parquet(str(config.resolved_quality_output_dir))
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
        quality_output_dir=config.resolved_quality_output_dir,
        quality_report_path=config.output_dir / QUALITY_REPORT_FILE_NAME,
        input_row_count=metrics.input_row_count,
        output_row_count=metrics.output_row_count,
        quality_issue_row_count=metrics.quality_issue_row_count,
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
        _count_when(_is_quality_evidence_row()).alias("quality_issue_row_count"),
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
        quality_issue_row_count=_metric_value(metrics_row, "quality_issue_row_count"),
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


def _build_quality_report(ranked_dataframe: Any, result: SilverTransactionsResult) -> dict[str, Any]:
    """Build a detailed data quality report for one Silver run."""

    return {
        "report_version": 1,
        "generated_at": result.completed_at,
        "bronze_dir": str(result.bronze_dir),
        "silver_output_dir": str(result.output_dir),
        "quality_output_dir": str(result.quality_output_dir),
        "no_silent_drop_policy": (
            "Rows excluded from silver.transactions are written to the quality evidence output "
            "with _silver_record_action explaining whether they were quarantined or duplicate-rejected."
        ),
        "row_counts": {
            "input": result.input_row_count,
            "silver_output": result.output_row_count,
            "quality_evidence": result.quality_issue_row_count,
            "quarantined": result.quarantined_row_count,
            "warning_selected": result.warning_row_count,
            "valid_selected": result.valid_row_count,
            "duplicate_transaction_ids": result.duplicate_transaction_id_count,
            "duplicate_rows_removed_from_main": result.duplicate_rows_removed_count,
            "event_dates": result.event_date_count,
        },
        "record_action_counts": _collect_record_action_counts(ranked_dataframe),
        "quality_issue_counts": _collect_quality_issue_counts(ranked_dataframe),
        "nullable_field_missing_counts": _collect_nullable_field_missing_counts(ranked_dataframe),
        "required_transaction_key_rules": REQUIRED_TRANSACTION_KEY_RULES,
        "nullable_field_rules": NULLABLE_FIELD_RULES,
    }


def _collect_record_action_counts(ranked_dataframe: Any) -> dict[str, int]:
    """Count selected, quarantined, and duplicate-rejected row actions."""

    return _collect_count_by_column(
        ranked_dataframe.withColumn("_silver_record_action", _record_action()),
        "_silver_record_action",
    )


def _collect_quality_issue_counts(ranked_dataframe: Any) -> dict[str, int]:
    """Count quality issue code occurrences across ranked candidate rows."""

    from pyspark.sql import functions as spark_functions

    issue_counts = (
        ranked_dataframe.select(spark_functions.explode("quality_issue_codes").alias("quality_issue_code"))
        .groupBy("quality_issue_code")
        .count()
        .orderBy("quality_issue_code")
        .collect()
    )
    return {row["quality_issue_code"]: int(row["count"]) for row in issue_counts}


def _collect_nullable_field_missing_counts(ranked_dataframe: Any) -> dict[str, int]:
    """Count nulls in fields that are intentionally nullable in Silver."""

    from pyspark.sql import functions as spark_functions

    missing_count_expressions = [
        _count_when(spark_functions.col(rule["field"]).isNull()).alias(rule["field"])
        for rule in NULLABLE_FIELD_RULES
    ]
    metrics_row = ranked_dataframe.agg(*missing_count_expressions).first()
    return {rule["field"]: _metric_value(metrics_row, rule["field"]) for rule in NULLABLE_FIELD_RULES}


def _collect_count_by_column(dataframe: Any, column_name: str) -> dict[str, int]:
    """Return counts grouped by one small-cardinality column."""

    return {
        row[column_name]: int(row["count"])
        for row in dataframe.groupBy(column_name).count().orderBy(column_name).collect()
    }


def _write_summary(result: SilverTransactionsResult, output_dir: Path) -> None:
    """Write a JSON evidence summary next to the Silver Parquet partitions."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def _write_quality_report(report: dict[str, Any], report_path: Path) -> None:
    """Write the detailed Silver data quality report."""

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)


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
    parser.add_argument(
        "--quality-output-dir",
        type=Path,
        help=f"Optional Parquet output path for quality evidence rows. Defaults to output parent/{QUALITY_OUTPUT_DIR_NAME}.",
    )
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=sorted(SUPPORTED_WRITE_MODES), default=DEFAULT_WRITE_MODE)
    parser.add_argument("--processed-at", help="Optional ISO timestamp used for _silver_processed_at.")
    add_spark_ui_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Silver transaction deduplication from the command line."""

    args = build_parser().parse_args(argv)
    config = SilverTransactionsConfig(
        bronze_dir=args.bronze_dir,
        output_dir=args.output_dir,
        quality_output_dir=args.quality_output_dir,
        master=args.master,
        write_mode=args.write_mode,
        processed_at=_parse_datetime(args.processed_at) if args.processed_at else None,
        spark_ui=spark_ui_config_from_args(args),
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
