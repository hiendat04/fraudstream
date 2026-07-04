"""Verify that the local PySpark runtime can execute a basic Parquet job.

This module is intentionally small. It exists to prove the Spark dependency,
local master settings, and Parquet write/read path before the Bronze ingestion
job is implemented.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_MASTER = "local[*]"
DEFAULT_OUTPUT_DIR = Path("/tmp/fraudstream_spark_local_check")
APP_NAME = "FraudStreamSparkLocalCheck"


@dataclass(frozen=True)
class SparkCheckResult:
    """Summary of a local Spark smoke-check run."""

    app_name: str
    master: str
    output_dir: Path
    row_count: int
    amount_sum: float
    spark_version: str
    completed_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable summary."""

        return {
            "app_name": self.app_name,
            "master": self.master,
            "output_dir": str(self.output_dir),
            "row_count": self.row_count,
            "amount_sum": self.amount_sum,
            "spark_version": self.spark_version,
            "completed_at": self.completed_at,
        }


def run_spark_local_check(master: str = DEFAULT_MASTER, output_dir: Path = DEFAULT_OUTPUT_DIR) -> SparkCheckResult:
    """Run a minimal Spark job that writes and reads a local Parquet dataset."""

    spark = _build_spark_session(master)
    try:
        from pyspark.sql import functions as sql_functions

        rows = [
            ("txn_check_001", "cust_check_001", 19.99),
            ("txn_check_002", "cust_check_002", 42.50),
            ("txn_check_003", "cust_check_001", 7.25),
        ]
        columns = ["transaction_id", "customer_id", "amount"]

        output_dir.parent.mkdir(parents=True, exist_ok=True)
        dataframe = spark.createDataFrame(rows, columns)
        dataframe.write.mode("overwrite").parquet(str(output_dir))

        read_back = spark.read.parquet(str(output_dir))
        metrics = read_back.agg(sql_functions.sum("amount").alias("amount_sum")).collect()[0]

        return SparkCheckResult(
            app_name=APP_NAME,
            master=master,
            output_dir=output_dir,
            row_count=read_back.count(),
            amount_sum=round(float(metrics["amount_sum"]), 2),
            spark_version=spark.version,
            completed_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
    finally:
        spark.stop()


def _build_spark_session(master: str) -> Any:
    """Create a local Spark session or raise a clear setup error."""

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Run `uv sync --extra spark`, then retry this command."
        ) from exc

    return (
        SparkSession.builder.appName(APP_NAME)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the local Spark check."""

    parser = argparse.ArgumentParser(description="Verify local Spark execution with a small Parquet round trip.")
    parser.add_argument(
        "--master",
        default=DEFAULT_MASTER,
        help="Spark master URL. Defaults to local[*].",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Temporary Parquet output path used by the smoke check.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the local Spark smoke check from the command line."""

    args = build_parser().parse_args(argv)
    try:
        result = run_spark_local_check(master=args.master, output_dir=args.output_dir)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
