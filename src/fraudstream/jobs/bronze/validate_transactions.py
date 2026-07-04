"""Validate Bronze transaction Parquet against the raw CSV source.

This tool is intentionally independent from the ingestion summary. It reads the
raw source files and the Bronze Parquet output, then verifies that Bronze kept
the same row coverage, source-file coverage, partition coverage, and raw format
issues expected from a preservation layer.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from fraudstream.jobs.bronze.ingest_transactions import (
    DEFAULT_MASTER,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SOURCE_DIR,
    SCHEMA_VERSION_V1,
    SCHEMA_VERSION_V2,
    TRANSACTION_FILE_GLOB,
)


APP_NAME = "FraudStreamBronzeTransactionValidation"
FORMAT_ISSUE_NAMES = (
    "padded_city_rows",
    "uppercase_city_rows",
    "lowercase_currency_rows",
    "uppercase_status_rows",
    "blank_city_rows",
    "blank_merchant_id_rows",
    "v1_missing_device_id_rows",
    "v2_blank_device_id_rows",
)
PARTITION_COLUMNS = ("schema_version", "transaction_date")


@dataclass(frozen=True)
class BronzeValidationConfig:
    """Runtime settings for Bronze transaction validation."""

    source_dir: Path = DEFAULT_SOURCE_DIR
    bronze_dir: Path = DEFAULT_OUTPUT_DIR
    master: str = DEFAULT_MASTER
    report_path: Path | None = None

    def validate(self) -> None:
        """Raise when the validation inputs are not available."""

        if not self.source_dir.exists():
            raise FileNotFoundError(f"source_dir does not exist: {self.source_dir}")
        if not self.bronze_dir.exists():
            raise FileNotFoundError(f"bronze_dir does not exist: {self.bronze_dir}")


@dataclass(frozen=True)
class SourceValidationStats:
    """Raw source measurements used for Bronze reconciliation."""

    row_count: int
    csv_file_count: int
    partition_count: int
    format_issues: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-serializable source stats."""

        return {
            "row_count": self.row_count,
            "csv_file_count": self.csv_file_count,
            "partition_count": self.partition_count,
            "format_issues": dict(self.format_issues),
        }


@dataclass(frozen=True)
class BronzeValidationStats:
    """Bronze Parquet measurements used for source reconciliation."""

    row_count: int
    parquet_data_file_count: int
    distinct_source_file_count: int
    partition_count: int
    distinct_schema_date_partition_count: int
    format_issues: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-serializable Bronze stats."""

        return {
            "row_count": self.row_count,
            "parquet_data_file_count": self.parquet_data_file_count,
            "distinct_source_file_count": self.distinct_source_file_count,
            "partition_count": self.partition_count,
            "distinct_schema_date_partition_count": self.distinct_schema_date_partition_count,
            "format_issues": dict(self.format_issues),
        }


@dataclass(frozen=True)
class BronzeValidationResult:
    """Result of comparing raw source files to Bronze Parquet."""

    source_dir: Path
    bronze_dir: Path
    passed: bool
    checks: Mapping[str, Any]
    source: SourceValidationStats
    bronze: BronzeValidationStats
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable validation result."""

        return {
            "source_dir": str(self.source_dir),
            "bronze_dir": str(self.bronze_dir),
            "passed": self.passed,
            "checks": dict(self.checks),
            "source": self.source.to_dict(),
            "bronze": self.bronze.to_dict(),
            "completed_at": self.completed_at,
        }


def validate_bronze_transactions(config: BronzeValidationConfig) -> BronzeValidationResult:
    """Compare raw source CSV files with Bronze Parquet output."""

    config.validate()
    source_files = _discover_source_files(config.source_dir)
    source_stats = _collect_source_stats(source_files)

    spark = _build_spark_session(config.master)
    try:
        bronze_stats = _collect_bronze_stats(spark, config.bronze_dir)
    finally:
        spark.stop()

    checks = _build_validation_checks(source_stats, bronze_stats)
    result = BronzeValidationResult(
        source_dir=config.source_dir,
        bronze_dir=config.bronze_dir,
        passed=_all_checks_passed(checks),
        checks=checks,
        source=source_stats,
        bronze=bronze_stats,
        completed_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    )

    if config.report_path is not None:
        _write_report(result, config.report_path)

    return result


def _discover_source_files(source_dir: Path) -> tuple[Path, ...]:
    """Return source CSV files from manifest when available, otherwise glob."""

    manifest_path = source_dir / "_manifest.json"
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as file:
            manifest = json.load(file)
        files = tuple(_resolve_source_file(path, source_dir) for path in manifest.get("files", []))
    else:
        files = tuple(sorted(source_dir.glob(TRANSACTION_FILE_GLOB)))

    if not files:
        raise FileNotFoundError(f"No source CSV files found under {source_dir}")

    missing_files = [path for path in files if not path.exists()]
    if missing_files:
        raise FileNotFoundError(f"Source file does not exist: {missing_files[0]}")

    return tuple(sorted(files))


def _resolve_source_file(raw_path: str, source_dir: Path) -> Path:
    """Resolve a manifest file path relative to the validation source directory."""

    path = Path(raw_path)
    if path.is_absolute() or path.exists():
        return path

    source_relative_path = source_dir / path
    if source_relative_path.exists():
        return source_relative_path

    return path


def _collect_source_stats(source_files: Sequence[Path]) -> SourceValidationStats:
    """Collect row, partition, and raw-quality counts directly from CSV files."""

    row_count = 0
    format_issues = _empty_format_issue_counts()
    partitions: set[tuple[str, str]] = set()

    for path in source_files:
        schema_version = _extract_partition_value(path, "schema_version")
        transaction_date = _extract_partition_value(path, "transaction_date")
        partitions.add((schema_version, transaction_date))

        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames is None:
                raise ValueError(f"Source CSV file is missing a header: {path}")

            has_device_id_column = "device_id" in reader.fieldnames
            for row in reader:
                row_count += 1
                _count_source_format_issues(format_issues, schema_version, has_device_id_column, row)

    return SourceValidationStats(
        row_count=row_count,
        csv_file_count=len(source_files),
        partition_count=len(partitions),
        format_issues=format_issues,
    )


def _count_source_format_issues(
    format_issues: dict[str, int],
    schema_version: str,
    has_device_id_column: bool,
    row: Mapping[str, str | None],
) -> None:
    """Increment raw-quality counters for one source row."""

    city = row.get("city") or ""
    currency = row.get("currency") or ""
    transaction_status = row.get("transaction_status") or ""
    merchant_id = row.get("merchant_id") or ""

    if city != city.strip():
        format_issues["padded_city_rows"] += 1
    if city and city == city.upper():
        format_issues["uppercase_city_rows"] += 1
    if currency and currency == currency.lower():
        format_issues["lowercase_currency_rows"] += 1
    if transaction_status and transaction_status == transaction_status.upper():
        format_issues["uppercase_status_rows"] += 1
    if city == "":
        format_issues["blank_city_rows"] += 1
    if merchant_id == "":
        format_issues["blank_merchant_id_rows"] += 1
    if schema_version == SCHEMA_VERSION_V1 and not has_device_id_column:
        format_issues["v1_missing_device_id_rows"] += 1
    if schema_version == SCHEMA_VERSION_V2 and row.get("device_id") == "":
        format_issues["v2_blank_device_id_rows"] += 1


def _collect_bronze_stats(spark: Any, bronze_dir: Path) -> BronzeValidationStats:
    """Collect row, partition, and raw-quality counts from Bronze Parquet."""

    from pyspark.sql import functions as spark_functions

    bronze_dataframe = spark.read.parquet(str(bronze_dir))
    checks = bronze_dataframe.agg(
        spark_functions.count("*").alias("row_count"),
        spark_functions.countDistinct("_source_file_path").alias("distinct_source_file_count"),
        spark_functions.countDistinct(*PARTITION_COLUMNS).alias("distinct_schema_date_partition_count"),
        spark_functions.sum(
            spark_functions.when(spark_functions.col("city").rlike(r"^\s+|\s+$"), 1).otherwise(0)
        ).alias("padded_city_rows"),
        spark_functions.sum(
            spark_functions.when(
                (spark_functions.col("city") != "")
                & (spark_functions.col("city") == spark_functions.upper(spark_functions.col("city"))),
                1,
            ).otherwise(0)
        ).alias("uppercase_city_rows"),
        spark_functions.sum(
            spark_functions.when(
                (spark_functions.col("currency") != "")
                & (spark_functions.col("currency") == spark_functions.lower(spark_functions.col("currency"))),
                1,
            ).otherwise(0)
        ).alias("lowercase_currency_rows"),
        spark_functions.sum(
            spark_functions.when(
                (spark_functions.col("transaction_status") != "")
                & (
                    spark_functions.col("transaction_status")
                    == spark_functions.upper(spark_functions.col("transaction_status"))
                ),
                1,
            ).otherwise(0)
        ).alias("uppercase_status_rows"),
        spark_functions.sum(spark_functions.when(spark_functions.col("city") == "", 1).otherwise(0)).alias(
            "blank_city_rows"
        ),
        spark_functions.sum(spark_functions.when(spark_functions.col("merchant_id") == "", 1).otherwise(0)).alias(
            "blank_merchant_id_rows"
        ),
        spark_functions.sum(
            spark_functions.when(
                (spark_functions.col("schema_version") == SCHEMA_VERSION_V1)
                & spark_functions.col("device_id").isNull(),
                1,
            ).otherwise(0)
        ).alias("v1_missing_device_id_rows"),
        spark_functions.sum(
            spark_functions.when(
                (spark_functions.col("schema_version") == SCHEMA_VERSION_V2)
                & (spark_functions.col("device_id") == ""),
                1,
            ).otherwise(0)
        ).alias("v2_blank_device_id_rows"),
    ).first()

    format_issues = {name: _safe_int(checks[name]) for name in FORMAT_ISSUE_NAMES}
    return BronzeValidationStats(
        row_count=_safe_int(checks["row_count"]),
        parquet_data_file_count=_count_bronze_data_files(bronze_dir),
        distinct_source_file_count=_safe_int(checks["distinct_source_file_count"]),
        partition_count=_count_bronze_partition_dirs(bronze_dir),
        distinct_schema_date_partition_count=_safe_int(checks["distinct_schema_date_partition_count"]),
        format_issues=format_issues,
    )


def _build_validation_checks(
    source_stats: SourceValidationStats,
    bronze_stats: BronzeValidationStats,
) -> dict[str, Any]:
    """Build pass/fail checks from source and Bronze stats."""

    format_issue_checks = {
        name: _comparison_check(source_stats.format_issues[name], bronze_stats.format_issues[name])
        for name in FORMAT_ISSUE_NAMES
    }

    return {
        "row_count": _comparison_check(source_stats.row_count, bronze_stats.row_count),
        "source_file_coverage": _comparison_check(
            source_stats.csv_file_count,
            bronze_stats.distinct_source_file_count,
        ),
        "partition_coverage": _comparison_check(
            source_stats.partition_count,
            bronze_stats.distinct_schema_date_partition_count,
        ),
        "bronze_partition_layout": _comparison_check(
            source_stats.partition_count,
            bronze_stats.partition_count,
        ),
        "raw_format_issue_preservation": {
            "passed": all(check["passed"] for check in format_issue_checks.values()),
            "issues": format_issue_checks,
        },
    }


def _comparison_check(expected: int, actual: int) -> dict[str, Any]:
    """Return a small expected-vs-actual validation check."""

    return {
        "expected": expected,
        "actual": actual,
        "passed": expected == actual,
    }


def _all_checks_passed(checks: Mapping[str, Any]) -> bool:
    """Return true when every validation check passed."""

    return all(_check_passed(check) for check in checks.values())


def _check_passed(check: Mapping[str, Any]) -> bool:
    """Return true for a simple or nested validation check."""

    if "passed" in check and isinstance(check["passed"], bool):
        return check["passed"]
    return all(_check_passed(value) for value in check.values() if isinstance(value, Mapping))


def _empty_format_issue_counts() -> dict[str, int]:
    """Return zero counters for all raw-quality checks."""

    return {name: 0 for name in FORMAT_ISSUE_NAMES}


def _extract_partition_value(path: Path, partition_name: str) -> str:
    """Extract a partition value such as schema_version from a path."""

    prefix = f"{partition_name}="
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    raise ValueError(f"Path is missing {partition_name} partition: {path}")


def _count_bronze_data_files(bronze_dir: Path) -> int:
    """Count physical Bronze Parquet data files."""

    return sum(1 for _ in bronze_dir.glob("ingest_date=*/schema_version=*/transaction_date=*/*.parquet"))


def _count_bronze_partition_dirs(bronze_dir: Path) -> int:
    """Count physical Bronze transaction-date partition directories."""

    return len(
        {path.parent for path in bronze_dir.glob("ingest_date=*/schema_version=*/transaction_date=*/*.parquet")}
    )


def _safe_int(value: Any) -> int:
    """Convert Spark numeric values to plain Python integers."""

    return int(value or 0)


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
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _write_report(result: BronzeValidationResult, report_path: Path) -> None:
    """Write the validation result as JSON."""

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as file:
        json.dump(result.to_dict(), file, indent=2)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for Bronze validation."""

    parser = argparse.ArgumentParser(
        description="Validate Bronze transaction Parquet against raw source CSV files."
    )
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--report-path", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run Bronze transaction validation from the command line."""

    args = build_parser().parse_args(argv)
    config = BronzeValidationConfig(
        source_dir=args.source_dir,
        bronze_dir=args.bronze_dir,
        master=args.master,
        report_path=args.report_path,
    )

    try:
        result = validate_bronze_transactions(config)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(result.to_dict(), indent=2))
    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
