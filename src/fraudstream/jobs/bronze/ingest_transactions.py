"""Ingest raw offline transaction CSV files into Bronze Parquet.

Bronze is a raw-preservation layer. This job reads the source CSV partitions
produced by the offline generator, keeps business values as raw strings, adds
source metadata, and writes partitioned Parquet for later Silver processing.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

from fraudstream import storage
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
    truncate_tables_cascade,
    warehouse_config_from_args,
    write_iceberg_table,
    write_jdbc_table,
)


APP_NAME = "FraudStreamBronzeTransactionIngestion"
DEFAULT_MASTER = "local[*]"
DEFAULT_SOURCE_DIR = Path("data/raw_source/offline_transactions")
DEFAULT_SOURCE_URI = "s3a://fraudstream/raw/offline_transactions"
DEFAULT_OUTPUT_DIR = Path("data/bronze/raw_transactions")
DEFAULT_SOURCE_SYSTEM = "fraudstream_generator"
DEFAULT_SOURCE_DATASET = "offline_transactions"
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_bronze_ingestion_summary.json"
TRANSACTION_FILE_SUFFIX = "transactions.csv"
SCHEMA_VERSION_V1 = "v1"
SCHEMA_VERSION_V2 = "v2"
RAW_HASH_NULL_TOKEN = "<NULL>"
CSV_NULL_SENTINEL = "\u0000"
CSV_SOURCE_PATH_COLUMN = "_source_file_path"

BASE_COLUMNS = [
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
    "event_timestamp",
    "created_ts",
]

EVOLVED_COLUMNS = [
    "device_id",
    "ip_address",
    "authentication_method",
    "risk_signal_version",
]

RAW_COLUMNS = [*BASE_COLUMNS, *EVOLVED_COLUMNS]
METADATA_COLUMNS = [
    "_source_system",
    "_source_dataset",
    "_source_file_path",
    "_source_file_name",
    "_source_row_number",
    "_source_manifest_path",
    "_source_manifest_created_at",
    "_ingest_run_id",
    "_ingested_at",
    "_raw_record_hash",
    "_corrupt_record",
]
PARTITION_COLUMNS = ["ingest_date", "schema_version", "transaction_date"]
BRONZE_COLUMNS = [*RAW_COLUMNS, *METADATA_COLUMNS, *PARTITION_COLUMNS]
SUPPORTED_WRITE_MODES = {"append", "overwrite", "errorifexists", "ignore"}


@dataclass(frozen=True)
class BronzeIngestionConfig:
    """Runtime settings for the Bronze transaction ingestion job.

    `source_dir` is local -- it only holds the generator's `_manifest.json`
    evidence file (used to discover source files by default). The raw CSV
    partitions themselves live in MinIO under `source_uri`; the no-manifest
    fallback lists that prefix directly (see `fraudstream.storage`).
    """

    source_dir: Path = DEFAULT_SOURCE_DIR
    source_uri: str = DEFAULT_SOURCE_URI
    output_dir: Path = DEFAULT_OUTPUT_DIR
    manifest_path: Path | None = None
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    ingest_run_id: str | None = None
    ingest_date: str | None = None
    source_system: str = DEFAULT_SOURCE_SYSTEM
    source_dataset: str = DEFAULT_SOURCE_DATASET
    spark_ui: SparkUIConfig = field(default_factory=SparkUIConfig)
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    iceberg: IcebergCatalogConfig = field(default_factory=IcebergCatalogConfig)
    postgres: PostgresJdbcConfig = field(default_factory=PostgresJdbcConfig)
    write_to_postgres: bool = True

    def validate(self) -> None:
        """Raise ValueError when the ingestion config is not usable."""

        if self.write_mode not in SUPPORTED_WRITE_MODES:
            allowed = ", ".join(sorted(SUPPORTED_WRITE_MODES))
            raise ValueError(f"write_mode must be one of: {allowed}")
        if not self.source_dir.exists():
            raise FileNotFoundError(f"source_dir does not exist: {self.source_dir}")
        if self.manifest_path is not None and not self.manifest_path.exists():
            raise FileNotFoundError(f"manifest_path does not exist: {self.manifest_path}")
        self.spark_ui.validate()


@dataclass(frozen=True)
class SourceManifest:
    """File-discovery metadata read from the raw source manifest.

    `files` holds MinIO URIs (`s3a://...`, or `file://...` in tests) --
    already-resolved, absolute locations, not paths needing further
    resolution against a local directory.
    """

    path: Path | None
    created_at: str | None
    files: tuple[str, ...]


@dataclass(frozen=True)
class SourceFileGroups:
    """Source CSV file URIs grouped by supported schema version."""

    v1_files: tuple[str, ...]
    v2_files: tuple[str, ...]
    unknown_files: tuple[str, ...]

    @property
    def has_supported_files(self) -> bool:
        """Return true when at least one supported source file exists."""

        return bool(self.v1_files or self.v2_files)


@dataclass(frozen=True)
class SourceCsvSchemaGroup:
    """Source CSV file URIs that share the same physical header."""

    columns: tuple[str, ...]
    files: tuple[str, ...]


@dataclass(frozen=True)
class BronzeRunContext:
    """Resolved runtime values shared across one Bronze ingestion run."""

    manifest: SourceManifest
    ingest_run_id: str
    ingested_at: datetime
    ingest_date: str


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Summary of one Bronze ingestion run."""

    source_dir: Path
    source_uri: str
    output_dir: Path
    source_file_count: int
    row_count: int
    duplicate_transaction_id_count: int
    schema_versions: tuple[str, ...]
    transaction_date_count: int
    ingest_run_id: str
    ingest_date: str
    write_mode: str
    spark_version: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable ingestion summary."""

        return {
            "source_dir": str(self.source_dir),
            "source_uri": self.source_uri,
            "output_dir": str(self.output_dir),
            "source_file_count": self.source_file_count,
            "row_count": self.row_count,
            "duplicate_transaction_id_count": self.duplicate_transaction_id_count,
            "schema_versions": list(self.schema_versions),
            "transaction_date_count": self.transaction_date_count,
            "ingest_run_id": self.ingest_run_id,
            "ingest_date": self.ingest_date,
            "write_mode": self.write_mode,
            "spark_version": self.spark_version,
            "completed_at": self.completed_at,
        }


def ingest_transactions_to_bronze(config: BronzeIngestionConfig) -> BronzeIngestionResult:
    """Read raw transaction CSV partitions from MinIO and write the Bronze Iceberg table."""

    config.validate()
    spark = _build_spark_session(config.master, config.spark_ui, config.warehouse, config.iceberg, config.postgres)
    bronze_dataframe = None
    try:
        announce_spark_ui(spark, config.spark_ui)
        context = _build_run_context(config)

        raw_dataframe = _read_raw_source_files(spark, context.manifest.files, config.warehouse)
        enriched_dataframe = _add_bronze_metadata(
            raw_dataframe=raw_dataframe,
            context=context,
            source_system=config.source_system,
            source_dataset=config.source_dataset,
        )
        bronze_dataframe = _prepare_for_reuse(enriched_dataframe)

        set_spark_job_group(
            spark,
            "bronze-write-raw-transactions",
            "Bronze: parse raw CSV, preserve schema problems, add lineage, and write to the Iceberg table on MinIO",
        )
        _write_bronze_iceberg(bronze_dataframe, config)

        set_spark_job_group(
            spark,
            "bronze-profile-offline-problems",
            "Bronze: profile duplicates, schema versions, and transaction-date coverage",
        )
        result = _build_ingestion_result(
            bronze_dataframe=bronze_dataframe,
            config=config,
            context=context,
            spark_version=spark.version,
        )

        if config.write_to_postgres:
            set_spark_job_group(
                spark,
                "bronze-write-postgres",
                "Bronze: write raw_transaction_ingest_runs and raw_transactions directly to PostgreSQL",
            )
            _write_bronze_postgres(spark, bronze_dataframe, result, context, config)

        clear_spark_job_group(spark)
        _write_summary(result, config.output_dir)
        retain_spark_ui(spark, config.spark_ui)
        return result
    finally:
        if bronze_dataframe is not None:
            bronze_dataframe.unpersist(blocking=False)
        spark.stop()


def _build_run_context(config: BronzeIngestionConfig) -> BronzeRunContext:
    """Resolve manifest and ingestion timestamps for one run."""

    manifest = _discover_source_manifest(config)
    if not manifest.files:
        raise FileNotFoundError(f"No source CSV files found under {config.source_dir}")

    ingested_at = datetime.now(UTC)
    return BronzeRunContext(
        manifest=manifest,
        ingest_run_id=config.ingest_run_id or f"bronze_transactions_{uuid4().hex}",
        ingested_at=ingested_at,
        ingest_date=config.ingest_date or ingested_at.date().isoformat(),
    )


def _build_spark_session(
    master: str,
    spark_ui: SparkUIConfig | None = None,
    warehouse: WarehouseConfig | None = None,
    iceberg: IcebergCatalogConfig | None = None,
    postgres: PostgresJdbcConfig | None = None,
) -> Any:
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
    builder = configure_object_storage(builder, warehouse or WarehouseConfig())
    builder = configure_iceberg_catalog(builder, iceberg or IcebergCatalogConfig(), postgres or PostgresJdbcConfig())
    return configure_spark_builder(builder, spark_ui or SparkUIConfig()).getOrCreate()


def _discover_source_manifest(config: BronzeIngestionConfig) -> SourceManifest:
    """Return source files from the manifest when available, otherwise list MinIO."""

    manifest_path = config.manifest_path or config.source_dir / "_manifest.json"
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as file:
            manifest = json.load(file)
        files = tuple(manifest.get("files", []))
        missing_files = [uri for uri in files if not storage.object_exists(config.warehouse, uri)]
        if missing_files:
            raise FileNotFoundError(f"Manifest references a missing source file: {missing_files[0]}")
        return SourceManifest(
            path=manifest_path,
            created_at=manifest.get("created_at"),
            files=files,
        )

    files = tuple(storage.list_keys(config.warehouse, config.source_uri, suffix=TRANSACTION_FILE_SUFFIX))
    return SourceManifest(path=None, created_at=None, files=files)


def _read_raw_source_files(spark: Any, source_files: Sequence[str], warehouse: WarehouseConfig) -> Any:
    """Read source CSV files by schema version and union them into one DataFrame."""

    dataframes = []
    grouped_files = _group_source_files(source_files)

    if grouped_files.unknown_files:
        raise ValueError(f"Unsupported schema_version in source file path: {grouped_files.unknown_files[0]}")
    if not grouped_files.has_supported_files:
        raise FileNotFoundError("No v1 or v2 transaction CSV files were discovered")

    if grouped_files.v1_files:
        dataframes.extend(
            _read_versioned_csv_files(
                spark=spark,
                source_files=grouped_files.v1_files,
                required_columns=BASE_COLUMNS,
                allowed_columns=BASE_COLUMNS,
                warehouse=warehouse,
            )
        )
    if grouped_files.v2_files:
        dataframes.extend(
            _read_versioned_csv_files(
                spark=spark,
                source_files=grouped_files.v2_files,
                required_columns=BASE_COLUMNS,
                allowed_columns=RAW_COLUMNS,
                warehouse=warehouse,
            )
        )

    return _union_raw_dataframes(dataframes)


def _read_versioned_csv_files(
    spark: Any,
    source_files: Sequence[str],
    required_columns: Sequence[str],
    allowed_columns: Sequence[str],
    warehouse: WarehouseConfig,
) -> list[Any]:
    """Read versioned source files while allowing optional evolved columns."""

    return [
        _select_raw_columns(_read_csv_files(spark, schema_group.files, schema_group.columns))
        for schema_group in _group_files_by_header(source_files, required_columns, allowed_columns, warehouse)
    ]


def _group_files_by_header(
    source_files: Sequence[str],
    required_columns: Sequence[str],
    allowed_columns: Sequence[str],
    warehouse: WarehouseConfig,
) -> tuple[SourceCsvSchemaGroup, ...]:
    """Group files by physical CSV header after validating the source contract."""

    allowed_column_set = set(allowed_columns)
    grouped_files: dict[tuple[str, ...], list[str]] = {}

    for uri in source_files:
        header_columns = _read_csv_header(uri, warehouse)
        _validate_source_header(uri, header_columns, required_columns, allowed_columns)
        source_columns = tuple(column for column in header_columns if column in allowed_column_set)
        grouped_files.setdefault(source_columns, []).append(uri)

    return tuple(
        SourceCsvSchemaGroup(columns=columns, files=tuple(sorted(files)))
        for columns, files in sorted(grouped_files.items(), key=lambda item: item[0])
    )


def _read_csv_header(uri: str, warehouse: WarehouseConfig) -> tuple[str, ...]:
    """Read the physical CSV header from one source file in MinIO."""

    text = storage.get_text(warehouse, uri)
    reader = csv.reader(io.StringIO(text))
    try:
        return tuple(next(reader))
    except StopIteration as exc:
        raise ValueError(f"Source CSV file is empty: {uri}") from exc


def _validate_source_header(
    uri: str,
    header_columns: Sequence[str],
    required_columns: Sequence[str],
    allowed_columns: Sequence[str],
) -> None:
    """Validate a source header without requiring every evolved column."""

    header_column_set = set(header_columns)
    allowed_column_set = set(allowed_columns)
    duplicate_columns = _duplicate_values(header_columns)
    missing_required_columns = [column for column in required_columns if column not in header_column_set]
    unsupported_columns = [column for column in header_columns if column not in allowed_column_set]

    if duplicate_columns:
        raise ValueError(f"Source CSV file has duplicate columns {duplicate_columns}: {uri}")
    if missing_required_columns:
        raise ValueError(f"Source CSV file is missing required columns {missing_required_columns}: {uri}")
    if unsupported_columns:
        raise ValueError(f"Source CSV file has unsupported columns {unsupported_columns}: {uri}")


def _duplicate_values(values: Sequence[str]) -> list[str]:
    """Return duplicate values in first-seen order."""

    seen: set[str] = set()
    duplicates: list[str] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    return duplicates


def _union_raw_dataframes(dataframes: Sequence[Any]) -> Any:
    """Union raw DataFrames that may have different optional columns."""

    dataframe = dataframes[0]
    for next_dataframe in dataframes[1:]:
        dataframe = dataframe.unionByName(next_dataframe, allowMissingColumns=True)
    return _select_raw_columns(dataframe)


def _group_source_files(source_files: Sequence[str]) -> SourceFileGroups:
    """Group source file URIs by the schema version encoded in their partition path."""

    v1_files: list[str] = []
    v2_files: list[str] = []
    unknown_files: list[str] = []

    for uri in source_files:
        if _has_schema_version(uri, SCHEMA_VERSION_V1):
            v1_files.append(uri)
        elif _has_schema_version(uri, SCHEMA_VERSION_V2):
            v2_files.append(uri)
        else:
            unknown_files.append(uri)

    return SourceFileGroups(
        v1_files=tuple(v1_files),
        v2_files=tuple(v2_files),
        unknown_files=tuple(unknown_files),
    )


def _has_schema_version(uri: str, schema_version: str) -> bool:
    """Return true when the URI contains a schema-version partition marker."""

    return f"schema_version={schema_version}/" in uri


def _source_schema(column_names: Sequence[str]) -> Any:
    """Build a nullable string schema for source CSV fields."""

    from pyspark.sql import types as spark_types

    fields = [spark_types.StructField(column_name, spark_types.StringType(), nullable=True) for column_name in column_names]
    fields.append(spark_types.StructField("_corrupt_record", spark_types.StringType(), nullable=True))
    return spark_types.StructType(fields)


def _read_csv_files(spark: Any, source_files: Sequence[str], source_columns: Sequence[str]) -> Any:
    """Read CSV files using a raw-preserving parser configuration.

    `source_files` are already `s3a://`/`file://` URI strings, and Spark's
    own CSV reader already handles either scheme natively (via the Hadoop
    S3A config `configure_object_storage` sets up) -- no local-vs-MinIO
    branching needed here.
    """

    schema = _source_schema(source_columns)
    dataframe = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding", "UTF-8")
        .option("nullValue", CSV_NULL_SENTINEL)
        .option("emptyValue", "")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .schema(schema)
        .csv(list(source_files))
    )
    return dataframe.na.fill("", subset=list(source_columns))


def _prepare_for_reuse(dataframe: Any) -> Any:
    """Persist the Bronze DataFrame because writing and metric checks reuse it."""

    from pyspark import StorageLevel

    return dataframe.persist(StorageLevel.MEMORY_AND_DISK)


def _write_bronze_iceberg(bronze_dataframe: Any, config: BronzeIngestionConfig) -> None:
    """Write Bronze rows into the `bronze.raw_transactions` Iceberg table on MinIO."""

    write_iceberg_table(
        bronze_dataframe,
        f"{config.iceberg.catalog_name}.bronze.raw_transactions",
        PARTITION_COLUMNS,
        config.write_mode,
    )


def _write_bronze_postgres(
    spark: Any,
    bronze_dataframe: Any,
    result: BronzeIngestionResult,
    context: BronzeRunContext,
    config: BronzeIngestionConfig,
) -> None:
    """Write the ingest-run audit row and raw Bronze rows directly to PostgreSQL.

    `raw_transactions._ingest_run_id` references `raw_transaction_ingest_runs`,
    so on a full refresh both tables are truncated together first (PostgreSQL
    resolves the foreign-key order), then each table is appended in
    parent-then-child order so the reference is always valid on insert.
    """

    ingest_runs_table = "bronze.raw_transaction_ingest_runs"
    raw_transactions_table = "bronze.raw_transactions"

    if config.write_mode == "overwrite":
        truncate_tables_cascade(spark, [raw_transactions_table, ingest_runs_table], config.postgres)

    ingest_run_dataframe = _build_ingest_run_dataframe(spark, result, context, config)
    write_jdbc_table(ingest_run_dataframe, ingest_runs_table, config.postgres, mode="append")
    write_jdbc_table(_cast_bronze_dates_for_postgres(bronze_dataframe), raw_transactions_table, config.postgres, mode="append")


def _cast_bronze_dates_for_postgres(bronze_dataframe: Any) -> Any:
    """Cast Bronze's raw string date partitions to `date` for the Postgres DDL's `DATE` columns.

    `ingest_date`/`transaction_date` stay plain strings everywhere else --
    including the Iceberg table -- to match Bronze's raw-preservation
    contract; only the PostgreSQL write needs this cast, since
    `bronze.raw_transactions` declares both columns `DATE NOT NULL`.
    """

    from pyspark.sql import functions as spark_functions

    return bronze_dataframe.withColumn(
        "ingest_date", spark_functions.to_date("ingest_date")
    ).withColumn("transaction_date", spark_functions.to_date("transaction_date"))


def _build_ingest_run_dataframe(
    spark: Any,
    result: BronzeIngestionResult,
    context: BronzeRunContext,
    config: BronzeIngestionConfig,
) -> Any:
    """Build the single-row audit DataFrame for `bronze.raw_transaction_ingest_runs`."""

    from pyspark.sql import types as spark_types

    schema = spark_types.StructType(
        [
            spark_types.StructField("ingest_run_id", spark_types.StringType(), nullable=False),
            spark_types.StructField("source_system", spark_types.StringType(), nullable=False),
            spark_types.StructField("source_dataset", spark_types.StringType(), nullable=False),
            spark_types.StructField("source_path", spark_types.StringType(), nullable=False),
            spark_types.StructField("manifest_path", spark_types.StringType(), nullable=True),
            spark_types.StructField("ingest_date", spark_types.DateType(), nullable=False),
            spark_types.StructField("started_at", spark_types.TimestampType(), nullable=True),
            spark_types.StructField("completed_at", spark_types.TimestampType(), nullable=True),
            spark_types.StructField("row_count", spark_types.LongType(), nullable=False),
            spark_types.StructField("source_file_count", spark_types.LongType(), nullable=False),
            spark_types.StructField("status", spark_types.StringType(), nullable=False),
            spark_types.StructField("summary_json", spark_types.StringType(), nullable=False),
        ]
    )
    row = (
        context.ingest_run_id,
        config.source_system,
        config.source_dataset,
        config.source_uri,
        str(context.manifest.path) if context.manifest.path else None,
        date.fromisoformat(context.ingest_date),
        context.ingested_at,
        datetime.now(UTC),
        result.row_count,
        result.source_file_count,
        "success",
        json.dumps(result.to_dict()),
    )
    return spark.createDataFrame([row], schema=schema)


def _select_raw_columns(dataframe: Any) -> Any:
    """Return the DataFrame with all Bronze raw columns present in order."""

    from pyspark.sql import functions as spark_functions

    selected_columns = []
    for column_name in [*RAW_COLUMNS, "_corrupt_record"]:
        if column_name in dataframe.columns:
            selected_columns.append(spark_functions.col(column_name).cast("string").alias(column_name))
        else:
            selected_columns.append(spark_functions.lit(None).cast("string").alias(column_name))
    return dataframe.select(*selected_columns)


def _add_bronze_metadata(
    raw_dataframe: Any,
    context: BronzeRunContext,
    source_system: str,
    source_dataset: str,
) -> Any:
    """Add source metadata and partition columns to raw transaction rows."""

    from pyspark.sql import Window
    from pyspark.sql import functions as spark_functions

    source_path = spark_functions.input_file_name()
    metadata_dataframe = (
        raw_dataframe.withColumn(CSV_SOURCE_PATH_COLUMN, source_path)
        .withColumn("_source_file_name", spark_functions.regexp_extract(source_path, r"([^/]+)$", 1))
        .withColumn("schema_version", spark_functions.regexp_extract(source_path, r"schema_version=([^/]+)", 1))
        .withColumn("transaction_date", spark_functions.regexp_extract(source_path, r"transaction_date=([^/]+)", 1))
    )

    row_window = Window.partitionBy(CSV_SOURCE_PATH_COLUMN).orderBy(spark_functions.monotonically_increasing_id())
    metadata_dataframe = metadata_dataframe.withColumn("_source_row_number", spark_functions.row_number().over(row_window))

    hash_fields = [
        spark_functions.coalesce(spark_functions.col(column_name), spark_functions.lit(RAW_HASH_NULL_TOKEN))
        for column_name in RAW_COLUMNS
    ]

    return (
        metadata_dataframe.withColumn("_source_system", spark_functions.lit(source_system))
        .withColumn("_source_dataset", spark_functions.lit(source_dataset))
        .withColumn("_source_row_number", spark_functions.col("_source_row_number").cast("long"))
        .withColumn(
            "_source_manifest_path",
            spark_functions.lit(str(context.manifest.path) if context.manifest.path else None).cast("string"),
        )
        .withColumn(
            "_source_manifest_created_at",
            spark_functions.lit(context.manifest.created_at).cast("string"),
        )
        .withColumn("_ingest_run_id", spark_functions.lit(context.ingest_run_id))
        .withColumn("_ingested_at", spark_functions.lit(context.ingested_at).cast("timestamp"))
        .withColumn("_raw_record_hash", spark_functions.sha2(spark_functions.concat_ws("||", *hash_fields), 256))
        .withColumn("ingest_date", spark_functions.lit(context.ingest_date))
        .select(*BRONZE_COLUMNS)
    )


def _build_ingestion_result(
    bronze_dataframe: Any,
    config: BronzeIngestionConfig,
    context: BronzeRunContext,
    spark_version: str,
) -> BronzeIngestionResult:
    """Build a compact summary for the completed ingestion run."""

    row_count = bronze_dataframe.count()
    duplicate_transaction_id_count = _count_duplicate_transaction_ids(bronze_dataframe)
    schema_versions = _collect_distinct_values(bronze_dataframe, "schema_version")
    transaction_date_count = len(_collect_distinct_values(bronze_dataframe, "transaction_date"))

    return BronzeIngestionResult(
        source_dir=config.source_dir,
        source_uri=config.source_uri,
        output_dir=config.output_dir,
        source_file_count=len(context.manifest.files),
        row_count=row_count,
        duplicate_transaction_id_count=duplicate_transaction_id_count,
        schema_versions=schema_versions,
        transaction_date_count=transaction_date_count,
        ingest_run_id=context.ingest_run_id,
        ingest_date=context.ingest_date,
        write_mode=config.write_mode,
        spark_version=spark_version,
        completed_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    )


def _count_duplicate_transaction_ids(bronze_dataframe: Any) -> int:
    """Count transaction IDs that appear more than once in Bronze."""

    from pyspark.sql import functions as spark_functions

    return (
        bronze_dataframe.groupBy("transaction_id")
        .count()
        .where(spark_functions.col("count") > 1)
        .count()
    )


def _collect_distinct_values(dataframe: Any, column_name: str) -> tuple[str, ...]:
    """Collect sorted distinct non-null values from one DataFrame column."""

    from pyspark.sql import functions as spark_functions

    return tuple(
        row[column_name]
        for row in (
            dataframe.select(column_name)
            .where(spark_functions.col(column_name).isNotNull())
            .distinct()
            .orderBy(column_name)
            .collect()
        )
    )


def _write_summary(result: BronzeIngestionResult, output_dir: Path) -> None:
    """Write a JSON evidence summary next to the Bronze Parquet partitions."""

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / SUMMARY_FILE_NAME).open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for Bronze transaction ingestion."""

    parser = argparse.ArgumentParser(description="Ingest raw transaction CSV files into the Bronze Iceberg table.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Local directory holding the generator's _manifest.json evidence file.",
    )
    parser.add_argument(
        "--source-uri",
        default=DEFAULT_SOURCE_URI,
        help="MinIO location of the raw CSV partitions, used when no manifest is found.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--manifest-path", type=Path)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=sorted(SUPPORTED_WRITE_MODES), default=DEFAULT_WRITE_MODE)
    parser.add_argument("--ingest-run-id")
    parser.add_argument("--ingest-date")
    add_spark_ui_arguments(parser)
    add_warehouse_arguments(parser)
    add_iceberg_arguments(parser)
    add_postgres_arguments(parser)
    parser.add_argument(
        "--skip-postgres-write",
        action="store_true",
        help="Skip the direct JDBC write to PostgreSQL (useful for local runs without a database).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Bronze transaction ingestion from the command line."""

    args = build_parser().parse_args(argv)
    config = BronzeIngestionConfig(
        source_dir=args.source_dir,
        source_uri=args.source_uri,
        output_dir=args.output_dir,
        manifest_path=args.manifest_path,
        master=args.master,
        write_mode=args.write_mode,
        ingest_run_id=args.ingest_run_id,
        ingest_date=args.ingest_date,
        spark_ui=spark_ui_config_from_args(args),
        warehouse=warehouse_config_from_args(args),
        iceberg=iceberg_config_from_args(args),
        postgres=postgres_config_from_args(args),
        write_to_postgres=not args.skip_postgres_write,
    )
    try:
        result = ingest_transactions_to_bronze(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
