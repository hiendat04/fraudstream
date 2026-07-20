"""Build offline fraud feature tables from persisted core Gold facts."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fraudstream.jobs.bronze.ingest_transactions import DEFAULT_MASTER, SUPPORTED_WRITE_MODES
from fraudstream.jobs.gold.transactions import (
    OFFLINE_FEATURE_TABLE_NAMES,
    _build_feat_customer_rolling,
    _build_feat_customer_total_orders_90d,
    _build_feat_merchant_risk_rolling,
    _build_feat_transaction_training,
    _build_merchant_category_daily,
    _build_merchant_category_rolling,
    _parse_datetime,
    _select_gold_columns,
    _to_utc_string,
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


APP_NAME = "FraudStreamOfflineFeatures"
DEFAULT_GOLD_DIR = Path("data/gold")
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_offline_features_summary.json"
REQUIRED_CORE_TABLE_NAMES = (
    "fact_transactions",
    "fact_customer_daily",
    "fact_account_daily",
    "fact_merchant_daily",
)


@dataclass(frozen=True)
class OfflineFeatureConfig:
    """Runtime settings for the persisted Gold-to-feature build."""

    gold_dir: Path = DEFAULT_GOLD_DIR
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    processed_at: datetime | None = None
    spark_ui: SparkUIConfig = field(default_factory=SparkUIConfig)

    def validate(self) -> None:
        """Raise when required core Gold inputs or runtime values are invalid."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        missing_tables = [
            table_name
            for table_name in REQUIRED_CORE_TABLE_NAMES
            if not (self.gold_dir / table_name).exists()
        ]
        if missing_tables:
            missing_text = ", ".join(missing_tables)
            raise FileNotFoundError(f"required core Gold tables are missing: {missing_text}")
        self.spark_ui.validate()


@dataclass(frozen=True)
class OfflineFeatureTableResult:
    """Materialization metrics for one offline feature table."""

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
class OfflineFeatureResult:
    """Evidence summary for one offline feature build."""

    gold_dir: Path
    write_mode: str
    spark_version: str
    processed_at: str
    completed_at: str
    source_fact_transaction_count: int
    training_distinct_transaction_count: int
    table_results: tuple[OfflineFeatureTableResult, ...]

    @property
    def training_row_count(self) -> int:
        """Return the transaction-training feature row count."""

        for table in self.table_results:
            if table.table_name == "feat_transaction_training":
                return table.row_count
        return 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable feature-build summary."""

        return {
            "gold_dir": str(self.gold_dir),
            "write_mode": self.write_mode,
            "spark_version": self.spark_version,
            "processed_at": self.processed_at,
            "completed_at": self.completed_at,
            "source_fact_transaction_count": self.source_fact_transaction_count,
            "training_row_count": self.training_row_count,
            "training_distinct_transaction_count": self.training_distinct_transaction_count,
            "training_row_count_matches_fact": (
                self.training_row_count == self.source_fact_transaction_count
            ),
            "training_transaction_ids_are_unique": (
                self.training_row_count == self.training_distinct_transaction_count
            ),
            "tables": [table.to_dict() for table in self.table_results],
        }


@dataclass(frozen=True)
class OfflineFeatureFrames:
    """Feature DataFrames created before materialization."""

    feat_customer_rolling: Any
    feat_customer_total_orders_90d: Any
    feat_merchant_risk_rolling: Any
    feat_transaction_training: Any


def build_offline_features(config: OfflineFeatureConfig) -> OfflineFeatureResult:
    """Read validated core Gold facts and materialize offline feature tables."""

    config.validate()
    spark = _build_spark_session(config.master, config.spark_ui)
    persisted_frames: list[Any] = []
    try:
        announce_spark_ui(spark, config.spark_ui)
        processed_at = config.processed_at or datetime.now(UTC)
        fact_transactions = spark.read.parquet(str(config.gold_dir / "fact_transactions"))
        fact_customer_daily = spark.read.parquet(str(config.gold_dir / "fact_customer_daily"))
        fact_account_daily = spark.read.parquet(str(config.gold_dir / "fact_account_daily"))
        fact_merchant_daily = spark.read.parquet(str(config.gold_dir / "fact_merchant_daily"))

        merchant_category_rolling = _build_merchant_category_rolling(
            _build_merchant_category_daily(fact_transactions)
        )
        feat_customer_rolling = _build_feat_customer_rolling(
            fact_customer_daily,
            processed_at,
        )
        feat_merchant_risk_rolling = _build_feat_merchant_risk_rolling(
            fact_merchant_daily=fact_merchant_daily,
            merchant_category_rolling=merchant_category_rolling,
            processed_at=processed_at,
        )
        frames = OfflineFeatureFrames(
            feat_customer_rolling=feat_customer_rolling,
            feat_customer_total_orders_90d=_build_feat_customer_total_orders_90d(
                fact_customer_daily,
                processed_at,
            ),
            feat_merchant_risk_rolling=feat_merchant_risk_rolling,
            feat_transaction_training=_build_feat_transaction_training(
                fact_transactions=fact_transactions,
                feat_customer_rolling=feat_customer_rolling,
                fact_account_daily=fact_account_daily,
                feat_merchant_risk_rolling=feat_merchant_risk_rolling,
                merchant_category_rolling=merchant_category_rolling,
                processed_at=processed_at,
            ),
        )

        persisted_frames.extend(
            [
                fact_transactions,
                fact_customer_daily,
                merchant_category_rolling,
                frames.feat_customer_rolling,
                frames.feat_merchant_risk_rolling,
            ]
        )
        for dataframe in persisted_frames:
            dataframe.persist()

        set_spark_job_group(
            spark,
            "offline-features-source-count",
            "Offline features: count source Gold transactions",
        )
        source_fact_transaction_count = fact_transactions.count()
        table_results, training_distinct_transaction_count = _write_feature_tables(
            frames,
            config,
        )
        result = OfflineFeatureResult(
            gold_dir=config.gold_dir,
            write_mode=config.write_mode,
            spark_version=spark.version,
            processed_at=_to_utc_string(processed_at),
            completed_at=_to_utc_string(datetime.now(UTC)),
            source_fact_transaction_count=source_fact_transaction_count,
            training_distinct_transaction_count=training_distinct_transaction_count,
            table_results=tuple(table_results),
        )
        _write_summary(result, config.gold_dir)
        clear_spark_job_group(spark)
        retain_spark_ui(spark, config.spark_ui)
        return result
    finally:
        for dataframe in persisted_frames:
            dataframe.unpersist(blocking=False)
        spark.stop()


def _build_spark_session(master: str, spark_ui: SparkUIConfig | None = None) -> Any:
    """Create the Spark session used by the offline feature job."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark`, then retry this command."
        ) from exc

    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )
    return configure_spark_builder(builder, spark_ui or SparkUIConfig()).getOrCreate()


def _write_feature_tables(
    frames: OfflineFeatureFrames,
    config: OfflineFeatureConfig,
) -> tuple[list[OfflineFeatureTableResult], int]:
    """Write feature frames and collect row-grain evidence for validation."""

    from pyspark.sql import functions as spark_functions

    table_writes = (
        ("feat_customer_rolling", frames.feat_customer_rolling, ("feature_date",)),
        ("feat_customer_total_orders_90d", frames.feat_customer_total_orders_90d, ()),
        ("feat_merchant_risk_rolling", frames.feat_merchant_risk_rolling, ("feature_date",)),
        ("feat_transaction_training", frames.feat_transaction_training, ()),
    )
    results: list[OfflineFeatureTableResult] = []
    training_distinct_transaction_count = 0
    shared_feature_tables = {
        "feat_customer_rolling",
        "feat_merchant_risk_rolling",
    }
    for table_name, dataframe, partition_columns in table_writes:
        output_path = config.gold_dir / table_name
        selected_dataframe = _select_gold_columns(dataframe, table_name)
        unpersist_after_write = table_name not in shared_feature_tables
        if unpersist_after_write:
            selected_dataframe.persist()
        try:
            set_spark_job_group(
                selected_dataframe.sparkSession,
                f"offline-features-build-{table_name}",
                f"Offline features: materialize and write {table_name}",
            )
            if table_name == "feat_transaction_training":
                metrics = selected_dataframe.agg(
                    spark_functions.count("*").alias("row_count"),
                    spark_functions.countDistinct("transaction_id").alias(
                        "distinct_transaction_count"
                    ),
                ).first()
                row_count = int(metrics["row_count"])
                training_distinct_transaction_count = int(
                    metrics["distinct_transaction_count"]
                )
            else:
                row_count = selected_dataframe.count()

            writer = selected_dataframe.write.mode(config.write_mode)
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.parquet(str(output_path))
            results.append(
                OfflineFeatureTableResult(
                    table_name=table_name,
                    output_path=output_path,
                    row_count=row_count,
                )
            )
        finally:
            if unpersist_after_write:
                selected_dataframe.unpersist(blocking=False)

    written_names = tuple(table.table_name for table in results)
    if written_names != OFFLINE_FEATURE_TABLE_NAMES:
        raise RuntimeError("offline feature table write order does not match its contract")
    return results, training_distinct_transaction_count


def _write_summary(result: OfflineFeatureResult, output_dir: Path) -> None:
    """Write the feature-build evidence summary next to its tables."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for offline feature materialization."""

    parser = argparse.ArgumentParser(
        description="Build offline feature tables from persisted core Gold facts."
    )
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument(
        "--write-mode",
        choices=sorted(SUPPORTED_WRITE_MODES),
        default=DEFAULT_WRITE_MODE,
    )
    parser.add_argument("--processed-at", help="Optional ISO timestamp used for feature metadata.")
    add_spark_ui_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the offline feature build from the command line."""

    args = build_parser().parse_args(argv)
    config = OfflineFeatureConfig(
        gold_dir=args.gold_dir,
        master=args.master,
        write_mode=args.write_mode,
        processed_at=_parse_datetime(args.processed_at) if args.processed_at else None,
        spark_ui=spark_ui_config_from_args(args),
    )
    try:
        result = build_offline_features(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
