"""Measure what the generated raw transactions look like, straight from the CSVs in MinIO.

The generator counts duplicates, late arrivals and skew as it writes, but some facts
need the whole dataset at once: how many distinct customers, accounts, merchants and
devices there are, and the columns each schema version really has. This job reads every file with
Spark and writes those facts to `_data_profile.json`. It also adds the distinct
counts to the generator's `_quality_summary.json`, where the data-quality report
reads them.

Run from the repo root. Credentials come from `. infra/vault/env.sh`. On the laptop:

    PYTHONPATH=src uv run --extra spark --extra storage python -m fraudstream.jobs.profile_raw_transactions --master "local[8]"

On the full dataset, inside the Airflow container:

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.profile_raw_transactions --master "local[8]"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from fraudstream import storage
from fraudstream.jobs.spark_settings import (
    add_retain_ui_argument,
    configure_spark_builder,
    keep_ui_open,
)
from fraudstream.jobs.warehouse import add_warehouse_arguments, configure_object_storage, warehouse_config_from_args

DEFAULT_SOURCE_DIR = Path("data/raw_source/offline_transactions")
ID_COLUMNS = ["customer_id", "account_id", "merchant_id", "device_id"]


def profile_raw_transactions(v1: Any, v2: Any, stored_bytes: int, file_count: int) -> dict[str, Any]:
    """Profile the old-schema (`v1`) and new-schema (`v2`) rows together in one read of the data.

    Duplicates, late arrivals and skew are already counted by the generator, so only
    what it can't count is measured here. Distinct ids are estimated within about 1%:
    an exact count would shuffle hundreds of millions of ids through disk. Blank
    values don't count as ids.
    """

    from pyspark.sql import functions as F

    rows = v1.withColumn("_schema", F.lit("v1")).unionByName(v2.withColumn("_schema", F.lit("v2")), allowMissingColumns=True)
    present = [column for column in ID_COLUMNS if column in rows.columns]
    totals = rows.agg(
        F.count("*").alias("rows"),
        F.sum((F.col("_schema") == "v1").cast("int")).alias("v1_rows"),
        *[F.approx_count_distinct(F.when(F.col(column) != "", F.col(column)), 0.01).alias(column) for column in present],
    ).first()
    row_count = totals["rows"]

    return {
        "rows": row_count,
        "bytes": stored_bytes,
        "files": file_count,
        "cardinality": {
            column: {"distinct": totals[column], "ratio": _rate(totals[column], row_count)} for column in present
        },
        "schema": {
            "v1": {"columns": v1.columns, "rows": totals["v1_rows"]},
            "v2": {"columns": v2.columns, "rows": row_count - totals["v1_rows"]},
        },
    }


def _rate(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile the generated raw transaction CSVs.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help="Local folder with the generator's _manifest.json; the profile is written next to it.",
    )
    parser.add_argument("--master", default="local[*]")
    add_retain_ui_argument(parser)
    add_warehouse_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Read the files the manifest lists, profile them, and write the results next to the manifest."""

    from pyspark.sql import SparkSession

    args = build_parser().parse_args(argv)
    warehouse = warehouse_config_from_args(args)
    files = json.loads((args.source_dir / "_manifest.json").read_text(encoding="utf-8"))["files"]

    builder = configure_object_storage(SparkSession.builder.appName("fraudstream-raw-profile").master(args.master), warehouse)
    spark = configure_spark_builder(builder, args.retain_ui).getOrCreate()
    try:
        read = spark.read.option("header", "true")
        profile = profile_raw_transactions(
            read.csv([uri for uri in files if "schema_version=v1/" in uri]),
            read.csv([uri for uri in files if "schema_version=v2/" in uri]),
            stored_bytes=sum(storage.object_size(warehouse, uri) for uri in files),
            file_count=len(files),
        )
        keep_ui_open(spark, args.retain_ui)
    finally:
        spark.stop()

    (args.source_dir / "_data_profile.json").write_text(json.dumps(profile, indent=2), encoding="utf-8")
    summary_path = args.source_dir / "_quality_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["high_cardinality"].update(
        {f"approx_count_distinct_{column}": values["distinct"] for column, values in profile["cardinality"].items()}
    )
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(profile, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
