"""Ingest raw offline transaction CSV files into Bronze Parquet.

Bronze is a raw-preservation layer. This job reads the source CSV partitions
produced by the offline generator, keeps business values as raw strings, adds
source metadata, and writes partitioned Parquet for later Silver processing.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

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


APP_NAME = "FraudStreamBronzeTransactionIngestion"
DEFAULT_MASTER = "local[*]"
DEFAULT_SOURCE_DIR = Path("data/raw_source/offline_transactions")
DEFAULT_OUTPUT_DIR = Path("data/bronze/raw_transactions")
DEFAULT_SOURCE_SYSTEM = "fraudstream_generator"
DEFAULT_SOURCE_DATASET = "offline_transactions"
DEFAULT_WRITE_MODE = "overwrite"
SUMMARY_FILE_NAME = "_bronze_ingestion_summary.json"
TRANSACTION_FILE_GLOB = "schema_version=*/transaction_date=*/transactions.csv"
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
    """Runtime settings for the Bronze transaction ingestion job."""

    source_dir: Path = DEFAULT_SOURCE_DIR
    output_dir: Path = DEFAULT_OUTPUT_DIR
    manifest_path: Path | None = None
    master: str = DEFAULT_MASTER
    write_mode: str = DEFAULT_WRITE_MODE
    ingest_run_id: str | None = None
    ingest_date: str | None = None
    source_system: str = DEFAULT_SOURCE_SYSTEM
    source_dataset: str = DEFAULT_SOURCE_DATASET
    spark_ui: SparkUIConfig = field(default_factory=SparkUIConfig)

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
    """File-discovery metadata read from the raw source manifest."""

    path: Path | None
    created_at: str | None
    files: tuple[Path, ...]


@dataclass(frozen=True)
class SourceFileGroups:
    """Source CSV files grouped by supported schema version."""

    v1_files: tuple[Path, ...]
    v2_files: tuple[Path, ...]
    unknown_files: tuple[Path, ...]

    @property
    def has_supported_files(self) -> bool:
        """Return true when at least one supported source file exists."""

        return bool(self.v1_files or self.v2_files)


@dataclass(frozen=True)
class SourceCsvSchemaGroup:
    """Source CSV files that share the same physical header."""

    columns: tuple[str, ...]
    files: tuple[Path, ...]


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
    """Read raw transaction CSV partitions and write Bronze Parquet."""

    config.validate()
    spark = _build_spark_session(config.master, config.spark_ui)
    bronze_dataframe = None
    try:
        announce_spark_ui(spark, config.spark_ui)
        context = _build_run_context(config)

        raw_dataframe = _read_raw_source_files(spark, context.manifest.files)
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
            "Bronze: parse raw CSV, preserve schema problems, add lineage, and write Parquet",
        )
        _write_bronze_parquet(bronze_dataframe, config)

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


def _discover_source_manifest(config: BronzeIngestionConfig) -> SourceManifest:
    """Return source files from the manifest when available, otherwise glob CSV files."""

    manifest_path = config.manifest_path or config.source_dir / "_manifest.json"
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as file:
            manifest = json.load(file)
        files = tuple(_resolve_source_file(path, config.source_dir) for path in manifest.get("files", []))
        missing_files = [path for path in files if not path.exists()]
        if missing_files:
            raise FileNotFoundError(f"Manifest references a missing source file: {missing_files[0]}")
        return SourceManifest(
            path=manifest_path,
            created_at=manifest.get("created_at"),
            files=files,
        )

    files = tuple(sorted(config.source_dir.glob(TRANSACTION_FILE_GLOB)))
    return SourceManifest(path=None, created_at=None, files=files)


def _resolve_source_file(raw_path: str, source_dir: Path) -> Path:
    """Resolve a source file path from manifest JSON into a local path."""

    path = Path(raw_path)
    if path.is_absolute():
        return path
    if path.exists():
        return path
    source_relative_path = source_dir / path
    if source_relative_path.exists():
        return source_relative_path
    file_name_candidate = source_dir / path.name
    return file_name_candidate if file_name_candidate.exists() else path


def _read_raw_source_files(spark: Any, source_files: Sequence[Path]) -> Any:
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
            )
        )
    if grouped_files.v2_files:
        dataframes.extend(
            _read_versioned_csv_files(
                spark=spark,
                source_files=grouped_files.v2_files,
                required_columns=BASE_COLUMNS,
                allowed_columns=RAW_COLUMNS,
            )
        )

    return _union_raw_dataframes(dataframes)


def _read_versioned_csv_files(
    spark: Any,
    source_files: Sequence[Path],
    required_columns: Sequence[str],
    allowed_columns: Sequence[str],
) -> list[Any]:
    """Read versioned source files while allowing optional evolved columns."""

    return [
        _select_raw_columns(_read_csv_files(spark, schema_group.files, schema_group.columns))
        for schema_group in _group_files_by_header(source_files, required_columns, allowed_columns)
    ]


def _group_files_by_header(
    source_files: Sequence[Path],
    required_columns: Sequence[str],
    allowed_columns: Sequence[str],
) -> tuple[SourceCsvSchemaGroup, ...]:
    """Group files by physical CSV header after validating the source contract."""

    allowed_column_set = set(allowed_columns)
    grouped_files: dict[tuple[str, ...], list[Path]] = {}

    for path in source_files:
        header_columns = _read_csv_header(path)
        _validate_source_header(path, header_columns, required_columns, allowed_columns)
        source_columns = tuple(column for column in header_columns if column in allowed_column_set)
        grouped_files.setdefault(source_columns, []).append(path)

    return tuple(
        SourceCsvSchemaGroup(columns=columns, files=tuple(sorted(files)))
        for columns, files in sorted(grouped_files.items(), key=lambda item: item[0])
    )


def _read_csv_header(path: Path) -> tuple[str, ...]:
    """Read the physical CSV header from one source file."""

    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        try:
            return tuple(next(reader))
        except StopIteration as exc:
            raise ValueError(f"Source CSV file is empty: {path}") from exc


def _validate_source_header(
    path: Path,
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
        raise ValueError(f"Source CSV file has duplicate columns {duplicate_columns}: {path}")
    if missing_required_columns:
        raise ValueError(f"Source CSV file is missing required columns {missing_required_columns}: {path}")
    if unsupported_columns:
        raise ValueError(f"Source CSV file has unsupported columns {unsupported_columns}: {path}")


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


def _group_source_files(source_files: Sequence[Path]) -> SourceFileGroups:
    """Group source files by the schema version encoded in their partition path."""

    v1_files: list[Path] = []
    v2_files: list[Path] = []
    unknown_files: list[Path] = []

    for path in source_files:
        if _has_schema_version(path, SCHEMA_VERSION_V1):
            v1_files.append(path)
        elif _has_schema_version(path, SCHEMA_VERSION_V2):
            v2_files.append(path)
        else:
            unknown_files.append(path)

    return SourceFileGroups(
        v1_files=tuple(v1_files),
        v2_files=tuple(v2_files),
        unknown_files=tuple(unknown_files),
    )


def _has_schema_version(path: Path, schema_version: str) -> bool:
    """Return true when the path contains a schema-version partition marker."""

    return f"schema_version={schema_version}" in path.parts


def _source_schema(column_names: Sequence[str]) -> Any:
    """Build a nullable string schema for source CSV fields."""

    from pyspark.sql import types as spark_types

    fields = [spark_types.StructField(column_name, spark_types.StringType(), nullable=True) for column_name in column_names]
    fields.append(spark_types.StructField("_corrupt_record", spark_types.StringType(), nullable=True))
    return spark_types.StructType(fields)


def _read_csv_files(spark: Any, source_files: Sequence[Path], source_columns: Sequence[str]) -> Any:
    """Read CSV files using a raw-preserving parser configuration."""

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
        .csv([str(path) for path in source_files])
    )
    return dataframe.na.fill("", subset=list(source_columns))


def _prepare_for_reuse(dataframe: Any) -> Any:
    """Persist the Bronze DataFrame because writing and metric checks reuse it."""

    from pyspark import StorageLevel

    return dataframe.persist(StorageLevel.MEMORY_AND_DISK)


def _write_bronze_parquet(bronze_dataframe: Any, config: BronzeIngestionConfig) -> None:
    """Write Bronze rows as partitioned Parquet."""

    (
        bronze_dataframe.write.mode(config.write_mode)
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(str(config.output_dir))
    )


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

    parser = argparse.ArgumentParser(description="Ingest raw transaction CSV files into Bronze Parquet.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--manifest-path", type=Path)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--write-mode", choices=sorted(SUPPORTED_WRITE_MODES), default=DEFAULT_WRITE_MODE)
    parser.add_argument("--ingest-run-id")
    parser.add_argument("--ingest-date")
    add_spark_ui_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Bronze transaction ingestion from the command line."""

    args = build_parser().parse_args(argv)
    config = BronzeIngestionConfig(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        manifest_path=args.manifest_path,
        master=args.master,
        write_mode=args.write_mode,
        ingest_run_id=args.ingest_run_id,
        ingest_date=args.ingest_date,
        spark_ui=spark_ui_config_from_args(args),
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
