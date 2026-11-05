"""Delete the files failed writes left in an Iceberg table's folder on MinIO.

A write puts its Parquet files on MinIO first and adds them to the table last, so a
job that dies in between leaves files no snapshot points at. Queries never see them,
but they still take up disk. Iceberg's own procedure compares the folder with every
snapshot and removes only what none of them reference.

Files of a write still in progress look exactly the same, and deleting them breaks
the table when that write commits. By default only files a day old are touched. A
shorter --older-than-hours clears a failed write straight away, so it refuses to run
while another Spark job is running. It lists what it would delete unless given --delete.

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.remove_orphan_files --table bronze.raw_transactions --older-than-hours 0
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from fraudstream.jobs.spark_settings import configure_spark_builder
from fraudstream.jobs.warehouse import (
    add_iceberg_arguments,
    add_postgres_arguments,
    add_warehouse_arguments,
    configure_iceberg_catalog,
    configure_object_storage,
    iceberg_config_from_args,
    postgres_config_from_args,
    warehouse_config_from_args,
)

DEFAULT_OLDER_THAN_HOURS = 24.0


def remove_orphan_files(spark: Any, catalog: str, table: str, older_than_hours: float, delete: bool) -> list[str]:
    """Return the table's orphan files older than the limit, deleting them only when `delete` is set.

    Iceberg's CALL procedure refuses a limit under 24 hours, so this goes through the
    action underneath it.
    """

    iceberg = spark._jvm.org.apache.iceberg.spark
    action = iceberg.actions.SparkActions.get(spark._jsparkSession).deleteOrphanFiles(
        iceberg.Spark3Util.loadIcebergTable(spark._jsparkSession, f"{catalog}.{table}")
    )
    action = action.olderThan(int((time.time() - older_than_hours * 3600) * 1000))
    if not delete:
        # Any Java Consumer stands in for the delete call; this one only collects the paths.
        action = action.deleteWith(spark._jvm.java.util.stream.Stream.builder())
    return list(action.execute().orphanFileLocations())


def other_spark_job_running() -> bool:
    """Starting this job cleared dead jobs' temp folders, so any other folder belongs to a live job."""

    own_folder = Path(os.environ.get("SPARK_LOCAL_DIRS", ""))
    return any(folder != own_folder for folder in own_folder.parent.glob("run-*"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete files in an Iceberg table's folder that no snapshot references.")
    parser.add_argument("--table", required=True, help="Table inside the catalog, such as bronze.raw_transactions.")
    parser.add_argument(
        "--older-than-hours",
        type=float,
        default=DEFAULT_OLDER_THAN_HOURS,
        help=f"Leave newer files alone. Defaults to {DEFAULT_OLDER_THAN_HOURS:g}.",
    )
    parser.add_argument("--delete", action="store_true", help="Delete the files. Without it they are only listed.")
    parser.add_argument("--master", default="local[*]")
    add_warehouse_arguments(parser)
    add_iceberg_arguments(parser)
    add_postgres_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = build_parser().parse_args(argv)
    iceberg = iceberg_config_from_args(args)
    builder = SparkSession.builder.appName("fraudstream-remove-orphan-files").master(args.master).config(
        "spark.sql.session.timeZone", "UTC"
    )
    builder = configure_object_storage(builder, warehouse_config_from_args(args))
    builder = configure_iceberg_catalog(builder, iceberg, postgres_config_from_args(args))
    spark = configure_spark_builder(builder, retain_ui=0).getOrCreate()
    try:
        if args.older_than_hours < DEFAULT_OLDER_THAN_HOURS and other_spark_job_running():
            raise SystemExit("Another Spark job is running. Its files look like orphans, so wait for it to finish.")
        files = remove_orphan_files(spark, iceberg.catalog_name, args.table, args.older_than_hours, args.delete)
    finally:
        spark.stop()

    print(json.dumps({"table": args.table, "deleted": args.delete, "orphan_files": len(files), "first_ten": files[:10]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
