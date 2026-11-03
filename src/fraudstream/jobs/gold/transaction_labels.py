"""Load the generator's raw training-label CSV into `gold.transaction_labels`.

Run from the repo root. Credentials come from `. infra/vault/env.sh`. On the laptop:

    PYTHONPATH=src uv run --extra spark --extra storage python -m fraudstream.jobs.gold.transaction_labels --write-mode overwrite

On the full dataset, inside the Airflow container:

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.gold.transaction_labels --write-mode overwrite
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fraudstream.generators.offline_transactions import DEFAULT_LABELS_URI
from fraudstream.jobs.bronze.ingest_transactions import DEFAULT_MASTER, SUPPORTED_WRITE_MODES
from fraudstream.jobs.gold.transactions import _parse_datetime, _to_utc_string
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
from fraudstream.jobs.spark_settings import add_spark_conf_argument, apply_spark_conf, parse_spark_conf
from fraudstream.jobs.warehouse import (
    IcebergCatalogConfig,
    PostgresJdbcConfig,
    WarehouseConfig,
    add_iceberg_arguments,
    add_postgres_arguments,
    add_warehouse_arguments,
    configure_iceberg_catalog,
    configure_object_storage,
    iceberg_config_from_args,
    postgres_config_from_args,
    warehouse_config_from_args,
    write_iceberg_table,
    postgres_window,
    write_postgres_copy,
)


APP_NAME = "FraudStreamTransactionLabels"
DEFAULT_GOLD_DIR = Path("data/gold")
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_transaction_labels_summary.json"
TABLE_NAME = "transaction_labels"


def _label_schema() -> Any:
    """Return the explicit schema for the generator's raw label CSV."""

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("is_fraud", IntegerType(), nullable=False),
            StructField("event_timestamp", TimestampType(), nullable=False),
        ]
    )


@dataclass(frozen=True)
class TransactionLabelsConfig:
    """Runtime settings for the transaction-labels load."""

    labels_uri: str = DEFAULT_LABELS_URI
    gold_dir: Path = DEFAULT_GOLD_DIR
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    processed_at: datetime | None = None
    spark_ui: SparkUIConfig = field(default_factory=SparkUIConfig)
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    iceberg: IcebergCatalogConfig = field(default_factory=IcebergCatalogConfig)
    postgres: PostgresJdbcConfig = field(default_factory=PostgresJdbcConfig)
    write_to_postgres: bool = True
    spark_conf: dict[str, str] = field(default_factory=dict)
    postgres_days: int = 0

    def validate(self) -> None:
        """Raise when required runtime values are invalid."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        self.spark_ui.validate()

    def gold_table_name(self) -> str:
        """Return the Iceberg-catalog-qualified name of the transaction-labels table."""

        return f"{self.iceberg.catalog_name}.gold.{TABLE_NAME}"


@dataclass(frozen=True)
class TransactionLabelsResult:
    """Evidence summary for one transaction-labels load."""

    labels_uri: str
    iceberg_table: str
    row_count: int
    distinct_transaction_id_count: int
    fraud_row_count: int
    write_mode: str
    spark_version: str
    processed_at: str
    completed_at: str
    spark_conf: dict[str, str] = field(default_factory=dict)
    postgres_rows: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable transaction-labels load summary."""

        return {
            "labels_uri": self.labels_uri,
            "iceberg_table": self.iceberg_table,
            "row_count": self.row_count,
            "distinct_transaction_id_count": self.distinct_transaction_id_count,
            "fraud_row_count": self.fraud_row_count,
            "write_mode": self.write_mode,
            "spark_version": self.spark_version,
            "spark_conf": self.spark_conf,
            "postgres_rows": self.postgres_rows,
            "processed_at": self.processed_at,
            "completed_at": self.completed_at,
        }


def build_transaction_labels(config: TransactionLabelsConfig) -> TransactionLabelsResult:
    """Read the generator's daily label files and materialize `gold.transaction_labels`."""

    from pyspark.sql import functions as spark_functions

    config.validate()
    spark = _build_spark_session(
        config.master, config.spark_ui, config.warehouse, config.iceberg, config.postgres, config.spark_conf
    )
    try:
        announce_spark_ui(spark, config.spark_ui)
        processed_at = config.processed_at or datetime.now(UTC)
        # One file per day under transaction_date=…/ folders; the date is already a column.
        dataframe = (
            spark.read.schema(_label_schema())
            .option("header", "true")
            .option("recursiveFileLookup", "true")
            .csv(config.labels_uri)
        )
        dataframe.persist()
        try:
            set_spark_job_group(
                spark,
                "transaction-labels-build",
                "Transaction labels: read raw label CSV and write to MinIO + PostgreSQL",
            )

            metrics = dataframe.agg(
                spark_functions.count("*").alias("row_count"),
                spark_functions.countDistinct("transaction_id").alias("distinct_transaction_id_count"),
                spark_functions.sum("is_fraud").alias("fraud_row_count"),
            ).first()
            row_count = int(metrics["row_count"])
            if row_count == 0:
                raise RuntimeError(f"No label rows found under {config.labels_uri}; refusing to write an empty label table.")
            distinct_transaction_id_count = int(metrics["distinct_transaction_id_count"])
            fraud_row_count = int(metrics["fraud_row_count"])

            iceberg_table = config.gold_table_name()
            write_iceberg_table(dataframe, iceberg_table, (), config.write_mode)
            postgres_rows = None
            if config.write_to_postgres:
                rows = postgres_window(spark.table(iceberg_table), "event_timestamp", config.postgres_days)
                postgres_rows = write_postgres_copy(rows, f"gold.{TABLE_NAME}", config.postgres)

            result = TransactionLabelsResult(
                labels_uri=config.labels_uri,
                iceberg_table=iceberg_table,
                row_count=row_count,
                distinct_transaction_id_count=distinct_transaction_id_count,
                fraud_row_count=fraud_row_count,
                write_mode=config.write_mode,
                spark_version=spark.version,
                spark_conf=config.spark_conf,
                postgres_rows=postgres_rows,
                processed_at=_to_utc_string(processed_at),
                completed_at=_to_utc_string(datetime.now(UTC)),
            )
            _write_summary(result, config.gold_dir)
            clear_spark_job_group(spark)
            retain_spark_ui(spark, config.spark_ui)
            return result
        finally:
            dataframe.unpersist(blocking=False)
    finally:
        spark.stop()


def _build_spark_session(
    master: str,
    spark_ui: SparkUIConfig | None = None,
    warehouse: WarehouseConfig | None = None,
    iceberg: IcebergCatalogConfig | None = None,
    postgres: PostgresJdbcConfig | None = None,
    spark_conf: dict[str, str] | None = None,
) -> Any:
    """Create the Spark session used by the transaction-labels job."""

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
        .config("spark.sql.session.timeZone", "UTC")
    )
    builder = configure_object_storage(builder, warehouse or WarehouseConfig())
    builder = configure_iceberg_catalog(builder, iceberg or IcebergCatalogConfig(), postgres or PostgresJdbcConfig())
    builder = configure_spark_builder(builder, spark_ui or SparkUIConfig())
    return apply_spark_conf(builder, spark_conf or {}).getOrCreate()


def _write_summary(result: TransactionLabelsResult, output_dir: Path) -> None:
    """Write the transaction-labels load evidence summary next to its table."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the transaction-labels load."""

    parser = argparse.ArgumentParser(
        description="Load the generator's raw training-label CSV into gold.transaction_labels."
    )
    parser.add_argument("--labels-uri", default=DEFAULT_LABELS_URI)
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument(
        "--write-mode",
        choices=sorted(SUPPORTED_WRITE_MODES),
        default=DEFAULT_WRITE_MODE,
    )
    parser.add_argument("--processed-at", help="Optional ISO timestamp used for load metadata.")
    add_spark_ui_arguments(parser)
    add_spark_conf_argument(parser)
    add_warehouse_arguments(parser)
    add_iceberg_arguments(parser)
    add_postgres_arguments(parser)
    parser.add_argument("--postgres-days", type=int, default=0, help="Days PostgreSQL keeps of transaction_labels; 0 keeps all.")
    parser.add_argument(
        "--skip-postgres-write",
        action="store_true",
        help="Skip the direct JDBC write to PostgreSQL (useful for local runs without a database).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the transaction-labels load from the command line."""

    args = build_parser().parse_args(argv)
    config = TransactionLabelsConfig(
        labels_uri=args.labels_uri,
        gold_dir=args.gold_dir,
        master=args.master,
        write_mode=args.write_mode,
        processed_at=_parse_datetime(args.processed_at) if args.processed_at else None,
        spark_ui=spark_ui_config_from_args(args),
        spark_conf=parse_spark_conf(args.spark_conf),
        warehouse=warehouse_config_from_args(args),
        iceberg=iceberg_config_from_args(args),
        postgres=postgres_config_from_args(args),
        write_to_postgres=not args.skip_postgres_write,
        postgres_days=args.postgres_days,
    )
    try:
        result = build_transaction_labels(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
