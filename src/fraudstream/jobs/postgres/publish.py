
"""Publish Silver and Gold Parquet tables into PostgreSQL.

Spark remains the system that builds Parquet datasets. This job creates the
serving copy in PostgreSQL so DBeaver, governance tools, and downstream services
can inspect relational tables with keys and constraints.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Sequence

from fraudstream.jobs.bronze.ingest_transactions import DEFAULT_MASTER


APP_NAME = "FraudStreamPostgresPublisher"
DEFAULT_SILVER_DIR = Path("data/silver/transactions")
DEFAULT_SILVER_QUALITY_DIR = Path("data/silver/transaction_quality_issues")
DEFAULT_GOLD_DIR = Path("data/gold")
DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DATABASE = "fraudstream"
DEFAULT_POSTGRES_USER = "fraudstream"
DEFAULT_POSTGRES_PASSWORD = "fraudstream_local_password"
DEFAULT_BATCH_SIZE = 1_000
SUPPORTED_LAYERS = ("silver", "gold", "silver-gold")
SUPPORTED_WRITE_MODES = ("append", "overwrite")

SILVER_TRANSACTION_COLUMNS = (
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
    "event_date",
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
)

SILVER_QUALITY_COLUMNS = (
    *SILVER_TRANSACTION_COLUMNS,
    "_silver_record_action",
    "_silver_quality_reported_at",
)

GOLD_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "dim_date": (
        "date_key",
        "event_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "day_of_month",
        "day_of_week",
        "day_name",
        "week_start_date",
        "month_start_date",
        "is_weekend",
    ),
    "dim_city": (
        "city_key",
        "city",
        "state_code",
        "country_code",
        "_gold_processed_at",
    ),
    "dim_channel": (
        "channel_key",
        "channel",
        "channel_group",
        "is_digital",
        "is_card_present",
        "description",
        "_gold_processed_at",
    ),
    "dim_quality_issue": (
        "quality_issue_code",
        "severity",
        "layer_origin",
        "description",
        "_gold_processed_at",
    ),
    "dim_merchant_category": (
        "merchant_category_key",
        "merchant_category",
        "category_group",
        "description",
        "_gold_processed_at",
    ),
    "dim_customer": (
        "customer_key",
        "customer_id",
        "first_seen_at",
        "last_seen_at",
        "first_event_date",
        "last_event_date",
        "primary_city_key",
        "primary_city",
        "account_count",
        "lifetime_transaction_count",
        "lifetime_amount",
        "average_transaction_amount",
        "fraud_transaction_count",
        "fraud_rate",
        "warning_transaction_count",
        "valid_from_ts",
        "valid_to_ts",
        "is_current",
        "_gold_processed_at",
    ),
    "dim_account": (
        "account_key",
        "account_id",
        "customer_key",
        "customer_id",
        "customer_count",
        "first_seen_at",
        "last_seen_at",
        "transaction_count",
        "lifetime_amount",
        "distinct_merchant_count",
        "fraud_transaction_count",
        "warning_transaction_count",
        "valid_from_ts",
        "valid_to_ts",
        "is_current",
        "_gold_processed_at",
    ),
    "dim_merchant": (
        "merchant_key",
        "merchant_dim_id",
        "merchant_id",
        "merchant_category_key",
        "merchant_category",
        "primary_city_key",
        "primary_city",
        "first_seen_at",
        "last_seen_at",
        "transaction_count",
        "distinct_customer_count",
        "lifetime_amount",
        "average_transaction_amount",
        "fraud_transaction_count",
        "fraud_rate",
        "warning_transaction_count",
        "valid_from_ts",
        "valid_to_ts",
        "is_current",
        "_gold_processed_at",
    ),
    "fact_transactions": (
        "transaction_id",
        "event_time",
        "event_date",
        "date_key",
        "customer_key",
        "customer_id",
        "account_key",
        "account_id",
        "merchant_key",
        "merchant_dim_id",
        "merchant_id",
        "merchant_category_key",
        "merchant_category",
        "city_key",
        "city",
        "channel_key",
        "channel",
        "transaction_status",
        "currency",
        "amount",
        "transaction_count",
        "is_approved",
        "is_declined",
        "is_reversed",
        "is_fraud",
        "source_created_at",
        "arrival_delay_minutes",
        "quality_status",
        "quality_issue_codes",
        "quality_issue_count",
        "duplicate_record_count",
        "_bronze_raw_record_hash",
        "_silver_processed_at",
        "_gold_processed_at",
    ),
    "fact_transaction_quality_issue": (
        "transaction_id",
        "quality_issue_code",
        "issue_position",
        "_gold_processed_at",
    ),
    "fact_customer_daily": (
        "customer_key",
        "customer_id",
        "date_key",
        "feature_date",
        "txn_count_1d",
        "approved_txn_count_1d",
        "declined_txn_count_1d",
        "reversed_txn_count_1d",
        "amount_sum_1d",
        "amount_avg_1d",
        "amount_max_1d",
        "distinct_merchant_count_1d",
        "distinct_city_count_1d",
        "online_txn_count_1d",
        "card_present_txn_count_1d",
        "fraud_txn_count_1d",
        "warning_txn_count_1d",
        "late_arrival_txn_count_1d",
        "_gold_processed_at",
    ),
    "fact_account_daily": (
        "account_key",
        "account_id",
        "customer_key",
        "customer_id",
        "date_key",
        "feature_date",
        "txn_count_1d",
        "amount_sum_1d",
        "amount_max_1d",
        "distinct_merchant_count_1d",
        "distinct_city_count_1d",
        "declined_txn_count_1d",
        "fraud_txn_count_1d",
        "_gold_processed_at",
    ),
    "fact_merchant_daily": (
        "merchant_key",
        "merchant_dim_id",
        "date_key",
        "feature_date",
        "merchant_category",
        "txn_count_1d",
        "amount_sum_1d",
        "amount_avg_1d",
        "distinct_customer_count_1d",
        "declined_txn_count_1d",
        "fraud_txn_count_1d",
        "fraud_rate_1d",
        "warning_txn_count_1d",
        "_gold_processed_at",
    ),
    "fact_city_category_daily": (
        "city_key",
        "merchant_category_key",
        "city",
        "merchant_category",
        "date_key",
        "feature_date",
        "txn_count_1d",
        "amount_sum_1d",
        "distinct_customer_count_1d",
        "distinct_merchant_count_1d",
        "fraud_txn_count_1d",
        "fraud_rate_1d",
        "_gold_processed_at",
    ),
    "fact_device_ip_daily": (
        "network_identifier",
        "identifier_type",
        "date_key",
        "feature_date",
        "txn_count_1d",
        "distinct_customer_count_1d",
        "distinct_account_count_1d",
        "distinct_merchant_count_1d",
        "fraud_txn_count_1d",
        "warning_txn_count_1d",
        "_gold_processed_at",
    ),
    "feat_customer_rolling": (
        "customer_key",
        "customer_id",
        "event_timestamp",
        "created",
        "feature_date",
        "window_start_date",
        "window_end_date",
        "txn_count_7d",
        "txn_count_30d",
        "amount_sum_7d",
        "amount_sum_30d",
        "amount_avg_7d",
        "amount_avg_30d",
        "distinct_merchant_count_7d",
        "distinct_merchant_count_30d",
        "declined_txn_count_7d",
        "fraud_txn_count_30d",
        "_gold_processed_at",
    ),
    "feat_customer_total_orders_90d": (
        "customer_key",
        "customer_id",
        "event_timestamp",
        "created",
        "total_orders_90d",
        "feature_window_start_ts",
        "feature_window_end_ts",
        "_gold_processed_at",
    ),
    "feat_transaction_training": (
        "transaction_id",
        "event_timestamp",
        "created",
        "customer_key",
        "account_key",
        "merchant_key",
        "date_key",
        "amount",
        "channel",
        "transaction_status",
        "customer_txn_count_7d",
        "customer_amount_sum_30d",
        "customer_distinct_merchant_count_7d",
        "account_txn_count_1d",
        "account_amount_sum_1d",
        "merchant_txn_count_1d",
        "merchant_fraud_rate_1d",
        "device_distinct_customer_count_1d",
        "ip_distinct_account_count_1d",
        "quality_issue_count",
        "duplicate_record_count",
        "arrival_delay_minutes",
        "is_fraud",
        "_gold_processed_at",
    ),
}


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the PostgreSQL serving database."""

    host: str = DEFAULT_POSTGRES_HOST
    port: int = DEFAULT_POSTGRES_PORT
    database: str = DEFAULT_POSTGRES_DATABASE
    user: str = DEFAULT_POSTGRES_USER
    password: str = DEFAULT_POSTGRES_PASSWORD


@dataclass(frozen=True)
class TablePublishSpec:
    """Mapping between one Parquet dataset and one PostgreSQL table."""

    name: str
    source_path: Path
    target_table: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class PostgresPublishConfig:
    """Runtime settings for publishing Parquet datasets to PostgreSQL."""

    layer: Literal["silver", "gold", "silver-gold"] = "silver"
    silver_dir: Path = DEFAULT_SILVER_DIR
    silver_quality_dir: Path = DEFAULT_SILVER_QUALITY_DIR
    gold_dir: Path = DEFAULT_GOLD_DIR
    master: str = DEFAULT_MASTER
    write_mode: Literal["append", "overwrite"] = "overwrite"
    batch_size: int = DEFAULT_BATCH_SIZE
    skip_missing: bool = False
    selected_tables: tuple[str, ...] = ()
    postgres: PostgresConfig = PostgresConfig()

    def validate(self) -> None:
        """Raise when the publish configuration is not usable."""

        if self.layer not in SUPPORTED_LAYERS:
            allowed = ", ".join(SUPPORTED_LAYERS)
            raise ValueError(f"layer must be one of: {allowed}")
        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(SUPPORTED_WRITE_MODES)
            raise ValueError(f"write_mode must be one of: {allowed}")
        if self.batch_size < 1:
            raise ValueError("batch_size must be greater than 0")


@dataclass(frozen=True)
class TablePublishResult:
    """Publish result for one target table."""

    name: str
    source_path: Path
    target_table: str
    status: Literal["published", "skipped"]
    row_count: int
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "name": self.name,
            "source_path": str(self.source_path),
            "target_table": self.target_table,
            "status": self.status,
            "row_count": self.row_count,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PostgresPublishResult:
    """Overall PostgreSQL publish result."""

    layer: str
    write_mode: str
    started_at: datetime
    completed_at: datetime
    table_results: tuple[TablePublishResult, ...]

    @property
    def published_row_count(self) -> int:
        """Return the total number of rows written to PostgreSQL."""

        return sum(table.row_count for table in self.table_results if table.status == "published")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "layer": self.layer,
            "write_mode": self.write_mode,
            "started_at": _to_utc_string(self.started_at),
            "completed_at": _to_utc_string(self.completed_at),
            "published_row_count": self.published_row_count,
            "tables": [table.to_dict() for table in self.table_results],
        }


def publish_parquet_to_postgres(config: PostgresPublishConfig) -> PostgresPublishResult:
    """Publish configured Silver and Gold Parquet datasets into PostgreSQL."""

    config.validate()
    started_at = datetime.now(UTC)
    table_specs = _select_table_specs(config)
    existing_specs, skipped_results = _resolve_existing_specs(table_specs, config.skip_missing)

    if not existing_specs and not skipped_results:
        raise ValueError("No PostgreSQL publish tables were selected.")
    if not existing_specs:
        completed_at = datetime.now(UTC)
        return PostgresPublishResult(
            layer=config.layer,
            write_mode=config.write_mode,
            started_at=started_at,
            completed_at=completed_at,
            table_results=tuple(skipped_results),
        )

    spark = _build_spark_session(config.master)
    try:
        return _publish_with_spark(
            spark=spark,
            config=config,
            table_specs=existing_specs,
            skipped_results=skipped_results,
            started_at=started_at,
        )
    finally:
        spark.stop()


def _publish_with_spark(
    *,
    spark: Any,
    config: PostgresPublishConfig,
    table_specs: Sequence[TablePublishSpec],
    skipped_results: Sequence[TablePublishResult],
    started_at: datetime,
) -> PostgresPublishResult:
    """Write selected table specs with an active Spark session."""

    published_results: list[TablePublishResult] = []
    with _connect(config.postgres) as connection:
        if table_specs and config.write_mode == "overwrite":
            _truncate_tables(connection, table_specs)

        for spec in table_specs:
            dataframe = _read_source_dataframe(spark, spec)
            row_count = _insert_dataframe(connection, dataframe, spec, config.batch_size)
            published_results.append(
                TablePublishResult(
                    name=spec.name,
                    source_path=spec.source_path,
                    target_table=spec.target_table,
                    status="published",
                    row_count=row_count,
                )
            )

    completed_at = datetime.now(UTC)
    return PostgresPublishResult(
        layer=config.layer,
        write_mode=config.write_mode,
        started_at=started_at,
        completed_at=completed_at,
        table_results=(*skipped_results, *published_results),
    )


def _select_table_specs(config: PostgresPublishConfig) -> tuple[TablePublishSpec, ...]:
    """Return table specs selected by layer and optional table filters."""

    specs: list[TablePublishSpec] = []
    if config.layer in {"silver", "silver-gold"}:
        specs.extend(_silver_table_specs(config))
    if config.layer in {"gold", "silver-gold"}:
        specs.extend(_gold_table_specs(config.gold_dir))

    selected = {_normalize_table_filter(value) for value in config.selected_tables}
    if selected:
        specs = [
            spec
            for spec in specs
            if spec.name in selected or spec.target_table in selected or _unqualified_table_name(spec.target_table) in selected
        ]
    return tuple(specs)


def _silver_table_specs(config: PostgresPublishConfig) -> tuple[TablePublishSpec, ...]:
    """Return Silver Parquet to PostgreSQL mappings."""

    return (
        TablePublishSpec(
            name="silver_transactions",
            source_path=config.silver_dir,
            target_table="silver.stg_transactions",
            columns=SILVER_TRANSACTION_COLUMNS,
        ),
        TablePublishSpec(
            name="silver_transaction_quality_issues",
            source_path=config.silver_quality_dir,
            target_table="silver.stg_transaction_quality_issues",
            columns=SILVER_QUALITY_COLUMNS,
        ),
    )


def _gold_table_specs(gold_dir: Path) -> tuple[TablePublishSpec, ...]:
    """Return Gold Parquet to PostgreSQL mappings in dependency order."""

    table_order = (
        "dim_date",
        "dim_city",
        "dim_channel",
        "dim_quality_issue",
        "dim_merchant_category",
        "dim_customer",
        "dim_account",
        "dim_merchant",
        "fact_transactions",
        "fact_transaction_quality_issue",
        "fact_customer_daily",
        "fact_account_daily",
        "fact_merchant_daily",
        "fact_city_category_daily",
        "fact_device_ip_daily",
        "feat_customer_rolling",
        "feat_customer_total_orders_90d",
        "feat_transaction_training",
    )
    return tuple(
        TablePublishSpec(
            name=table_name,
            source_path=gold_dir / table_name,
            target_table=f"gold.{table_name}",
            columns=GOLD_TABLE_COLUMNS[table_name],
        )
        for table_name in table_order
    )


def _resolve_existing_specs(
    table_specs: Sequence[TablePublishSpec],
    skip_missing: bool,
) -> tuple[tuple[TablePublishSpec, ...], tuple[TablePublishResult, ...]]:
    """Validate source paths and optionally skip missing Parquet datasets."""

    existing_specs: list[TablePublishSpec] = []
    skipped_results: list[TablePublishResult] = []
    missing_paths: list[str] = []

    for spec in table_specs:
        if _path_has_parquet_data(spec.source_path):
            existing_specs.append(spec)
        elif skip_missing:
            skipped_results.append(
                TablePublishResult(
                    name=spec.name,
                    source_path=spec.source_path,
                    target_table=spec.target_table,
                    status="skipped",
                    row_count=0,
                    reason="source parquet path does not exist or has no parquet files",
                )
            )
        else:
            missing_paths.append(f"{spec.target_table}: {spec.source_path}")

    if missing_paths:
        formatted_paths = "\n".join(f"- {path}" for path in missing_paths)
        raise FileNotFoundError(
            "Missing source Parquet data for PostgreSQL publish:\n"
            f"{formatted_paths}\n"
            "Run the upstream Spark job first, or pass --skip-missing while developing."
        )
    return tuple(existing_specs), tuple(skipped_results)


def _path_has_parquet_data(path: Path) -> bool:
    """Return true when a path exists and contains Parquet part files."""

    return path.exists() and any(path.rglob("*.parquet"))


def _read_source_dataframe(spark: Any, spec: TablePublishSpec) -> Any:
    """Read one source Parquet dataset and validate its schema."""

    dataframe = spark.read.parquet(str(spec.source_path))
    missing_columns = [column for column in spec.columns if column not in dataframe.columns]
    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(f"{spec.source_path} is missing columns for {spec.target_table}: {missing_text}")
    return dataframe.select(*spec.columns)


def _insert_dataframe(connection: Any, dataframe: Any, spec: TablePublishSpec, batch_size: int) -> int:
    """Stream Spark rows into PostgreSQL in batches."""

    row_count = 0
    insert_sql = _build_insert_sql(spec.target_table, spec.columns)
    with connection.cursor() as cursor:
        for batch in _iter_row_batches(dataframe, spec.columns, batch_size):
            cursor.executemany(insert_sql, batch)
            connection.commit()
            row_count += len(batch)
    return row_count


def _iter_row_batches(dataframe: Any, columns: Sequence[str], batch_size: int) -> Iterator[list[tuple[Any, ...]]]:
    """Yield normalized Spark rows in bounded batches."""

    batch: list[tuple[Any, ...]] = []
    for row in dataframe.toLocalIterator():
        row_dict = row.asDict(recursive=True)
        batch.append(tuple(_normalize_postgres_value(row_dict[column]) for column in columns))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _normalize_postgres_value(value: Any) -> Any:
    """Convert Spark/Python values into forms psycopg can adapt."""

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, date | Decimal | str | int | bool | float):
        return value
    if isinstance(value, list | tuple):
        return [_normalize_postgres_value(item) for item in value]
    return value


def _build_insert_sql(table_name: str, columns: Sequence[str]) -> str:
    """Build a parameterized INSERT statement for psycopg."""

    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {_quote_table_name(table_name)} ({quoted_columns}) VALUES ({placeholders})"


def _truncate_tables(connection: Any, table_specs: Sequence[TablePublishSpec]) -> None:
    """Truncate selected PostgreSQL targets before a full-refresh publish."""

    table_names = ", ".join(_quote_table_name(spec.target_table) for spec in table_specs)
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE")
    connection.commit()


def _connect(config: PostgresConfig) -> Any:
    """Open a psycopg connection or raise a clear dependency error."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not installed. Run `uv sync --extra postgres --extra spark`, then retry this command."
        ) from exc

    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        application_name=APP_NAME,
    )


def _build_spark_session(master: str) -> Any:
    """Create a Spark session for reading Parquet sources."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark --extra postgres`, then retry this command."
        ) from exc

    return (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _quote_table_name(table_name: str) -> str:
    """Quote a possibly schema-qualified PostgreSQL table name."""

    return ".".join(_quote_identifier(part) for part in table_name.split("."))


def _quote_identifier(identifier: str) -> str:
    """Return a safely quoted PostgreSQL identifier."""

    return '"' + identifier.replace('"', '""') + '"'


def _normalize_table_filter(value: str) -> str:
    """Normalize user-provided table filters."""

    return value.strip()


def _unqualified_table_name(table_name: str) -> str:
    """Return table name without schema prefix."""

    return table_name.split(".")[-1]


def _to_utc_string(value: datetime) -> str:
    """Format a datetime as a stable UTC ISO string."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_selected_tables(raw_value: str | None) -> tuple[str, ...]:
    """Parse a comma-separated table filter argument."""

    if not raw_value:
        return ()
    return tuple(value.strip() for value in raw_value.split(",") if value.strip())


def _env_value(name: str, default: str) -> str:
    """Read an environment variable with a default."""

    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable with a default."""

    raw_value = os.environ.get(name)
    return int(raw_value) if raw_value else default


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for PostgreSQL publishing."""

    parser = argparse.ArgumentParser(
        description="Publish Silver and Gold Parquet datasets into PostgreSQL serving tables."
    )
    parser.add_argument("--layer", choices=SUPPORTED_LAYERS, default="silver")
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--silver-quality-dir", type=Path, default=DEFAULT_SILVER_QUALITY_DIR)
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=SUPPORTED_WRITE_MODES, default="overwrite")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--tables",
        help=(
            "Optional comma-separated table filter. Accepts spec names, unqualified table names, "
            "or schema-qualified table names."
        ),
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip selected tables whose source Parquet path does not exist yet.",
    )
    parser.add_argument("--postgres-host", default=_env_value("POSTGRES_HOST", DEFAULT_POSTGRES_HOST))
    parser.add_argument("--postgres-port", type=int, default=_env_int("POSTGRES_PORT", DEFAULT_POSTGRES_PORT))
    parser.add_argument("--postgres-db", default=_env_value("POSTGRES_DB", DEFAULT_POSTGRES_DATABASE))
    parser.add_argument("--postgres-user", default=_env_value("POSTGRES_USER", DEFAULT_POSTGRES_USER))
    parser.add_argument(
        "--postgres-password",
        default=_env_value("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the PostgreSQL publisher from the command line."""

    args = build_parser().parse_args(argv)
    config = PostgresPublishConfig(
        layer=args.layer,
        silver_dir=args.silver_dir,
        silver_quality_dir=args.silver_quality_dir,
        gold_dir=args.gold_dir,
        master=args.master,
        write_mode=args.write_mode,
        batch_size=args.batch_size,
        skip_missing=args.skip_missing,
        selected_tables=_parse_selected_tables(args.tables),
        postgres=PostgresConfig(
            host=args.postgres_host,
            port=args.postgres_port,
            database=args.postgres_db,
            user=args.postgres_user,
            password=args.postgres_password,
        ),
    )
    try:
        result = publish_parquet_to_postgres(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
