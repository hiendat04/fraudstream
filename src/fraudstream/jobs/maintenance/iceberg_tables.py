"""Keep Iceberg tables fast to read: merge small files, cluster rows, drop old snapshots.

Each streaming commit and each feature append adds a few small files, and a query
opens every file it can't rule out, so small files slow every read. Compaction
rewrites them into files of about 128 MiB. Z-order rewrites a table so rows with
close values of the given columns share files, which lets a filter on those
columns skip most files. Every rewrite keeps the old files for time travel until
their snapshots expire, so expiry runs last and frees them.

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.maintenance.iceberg_tables --table iceberg.streaming.customer_features_5m
    ... --zorder iceberg.silver.stg_transactions=customer_id,merchant_id
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fraudstream.jobs.remove_orphan_files import remove_orphan_files
from fraudstream.jobs.spark_settings import (
    add_spark_conf_argument,
    apply_spark_conf,
    configure_spark_builder,
    parse_spark_conf,
)
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

TARGET_FILE_BYTES = 134_217_728
SMALL_FILE_BYTES = 32 * 1024 * 1024
ORPHAN_AGE_HOURS = 72
DEFAULT_OUTPUT_DIR = Path("data/maintenance")
REPORT_FILE_NAME = "_maintenance_report.json"
STEPS = ("compact", "expire")


def table_file_stats(spark: Any, table: str) -> dict[str, int]:
    """Count the current snapshot's data and delete files, and how big the data files are."""

    from pyspark.sql import functions as F

    stats = (
        spark.table(f"{table}.files")
        .agg(
            F.count(F.when(F.col("content") == 0, 1)).alias("data_files"),
            F.count(F.when(F.col("content") != 0, 1)).alias("delete_files"),
            F.sum(F.when(F.col("content") == 0, F.col("file_size_in_bytes"))).alias("total_bytes"),
            F.count(F.when((F.col("content") == 0) & (F.col("file_size_in_bytes") < SMALL_FILE_BYTES), 1)).alias("small_files"),
        )
        .first()
    )
    data_files, total_bytes = stats["data_files"], stats["total_bytes"] or 0
    return {
        "data_files": data_files,
        "delete_files": stats["delete_files"],
        "total_bytes": total_bytes,
        "average_file_bytes": total_bytes // data_files if data_files else 0,
        "small_files": stats["small_files"],
    }


def compact_table(spark: Any, table: str, target_file_bytes: int = TARGET_FILE_BYTES) -> dict[str, Any]:
    """Merge small data files with their deletes applied, then drop the delete files left pointing at nothing."""

    catalog, name = _split(table)
    data = _call(
        spark,
        f"{catalog}.system.rewrite_data_files(table => '{name}', strategy => 'binpack', options => map("
        f"'target-file-size-bytes', '{target_file_bytes}', "
        # Iceberg skips fewer than 5 files by default; any two small files are worth merging.
        "'min-input-files', '2', "
        # Flink's upserts leave delete files: rewrite every data file one applies to, and drop the equality deletes
        # that no longer apply to any file.
        "'delete-file-threshold', '1', 'remove-dangling-deletes', 'true'))",
    )
    # Position deletes still name the files just rewritten; rewriting them all drops the entries for files gone.
    deletes = _call(
        spark, f"{catalog}.system.rewrite_position_delete_files(table => '{name}', options => map('rewrite-all', 'true'))"
    )
    return {**data, "position_deletes": deletes}


def zorder_table(spark: Any, table: str, columns: Sequence[str], where: str | None = None) -> dict[str, Any]:
    """Rewrite the data files (only those `where` matches, if given) so rows close on all of `columns` share files."""

    catalog, name = _split(table)
    if where is not None and '"' in where:
        raise ValueError(f"write the filter with single quotes: {where!r}")
    only = f', where => "{where}"' if where else ""
    return _call(
        spark,
        f"{catalog}.system.rewrite_data_files(table => '{name}', strategy => 'sort', "
        f"sort_order => 'zorder({','.join(columns)})', options => map('rewrite-all', 'true'){only})",
    )


def expire_old_snapshots(spark: Any, table: str, retain_last: int = 5) -> dict[str, Any]:
    """Keep the last `retain_last` snapshots, then delete files no snapshot references after 3 days."""

    catalog, name = _split(table)
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    expired = _call(
        spark,
        f"{catalog}.system.expire_snapshots(table => '{name}', older_than => TIMESTAMP '{now}', retain_last => {retain_last})",
    )
    # Three days leaves any write still in progress alone.
    orphans = remove_orphan_files(spark, catalog, name, ORPHAN_AGE_HOURS, delete=True)
    return {**expired, "orphan_files_deleted": len(orphans)}


def maintain(
    spark: Any,
    tables: Sequence[str],
    zorder: dict[str, list[str]],
    steps: Sequence[str] = STEPS,
    zorder_where: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Compact (or z-order) each table, then expire its snapshots. A failing table doesn't stop the others."""

    report: dict[str, dict[str, Any]] = {}
    for table in dict.fromkeys([*tables, *zorder]):
        entry: dict[str, Any] = {"before": None, "after": None, "actions": {}, "error": None}
        report[table] = entry
        try:
            entry["before"] = table_file_stats(spark, table)
            if "compact" in steps and table in zorder:
                entry["actions"]["zorder"] = zorder_table(spark, table, zorder[table], zorder_where)
            elif "compact" in steps:
                entry["actions"]["compact"] = compact_table(spark, table)
            if "expire" in steps:
                entry["actions"]["expire_snapshots"] = expire_old_snapshots(spark, table)
            entry["after"] = table_file_stats(spark, table)
        except Exception as error:  # noqa: BLE001 - recorded in the report, and the next table still runs
            entry["error"] = f"{type(error).__name__}: {error}"
    return report


def report_failed(report: dict[str, dict[str, Any]]) -> bool:
    return bool(report) and all(entry["error"] for entry in report.values())


def parse_zorder(values: Sequence[str]) -> dict[str, list[str]]:
    """Turn `TABLE=col1,col2` flags into {table: [columns]}."""

    zorder = {}
    for value in values:
        table, _, columns = value.partition("=")
        names = [column.strip() for column in columns.split(",") if column.strip()]
        if not table or not names:
            raise ValueError(f"--zorder needs TABLE=col1,col2, got {value!r}")
        zorder[table] = names
    return zorder


def _split(table: str) -> tuple[str, str]:
    """`iceberg.streaming.x` -> (`iceberg`, `streaming.x`), as the CALL procedures want them."""

    catalog, _, name = table.partition(".")
    return catalog, name


def _call(spark: Any, procedure: str) -> dict[str, Any]:
    rows = spark.sql(f"CALL {procedure}").collect()
    return rows[0].asDict() if rows else {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compact, z-order and expire snapshots of Iceberg tables.")
    parser.add_argument("--table", action="append", default=[], help="Full table name, such as iceberg.streaming.x. Repeatable.")
    parser.add_argument("--zorder", action="append", default=[], help="TABLE=col1,col2: z-order this table instead of compacting it. Repeatable.")
    parser.add_argument(
        "--zorder-where", help="Z-order only the files this filter matches, such as \"event_date >= DATE '2026-03-01'\"."
    )
    parser.add_argument("--step", choices=[*STEPS, "all"], default="all", help="Run only compaction or only expiry.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default="local[*]")
    add_warehouse_arguments(parser)
    add_iceberg_arguments(parser)
    add_postgres_arguments(parser)
    add_spark_conf_argument(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        zorder = parse_zorder(args.zorder)
    except ValueError as error:
        parser.error(str(error))
    if not args.table and not zorder:
        parser.error("give at least one --table or --zorder")

    builder = SparkSession.builder.appName("fraudstream-iceberg-maintenance").master(args.master).config(
        "spark.sql.session.timeZone", "UTC"
    )
    builder = configure_object_storage(builder, warehouse_config_from_args(args))
    builder = configure_iceberg_catalog(builder, iceberg_config_from_args(args), postgres_config_from_args(args))
    spark = apply_spark_conf(configure_spark_builder(builder, retain_ui=0), parse_spark_conf(args.spark_conf)).getOrCreate()
    try:
        report = maintain(spark, args.table, zorder, STEPS if args.step == "all" else (args.step,), args.zorder_where)
    finally:
        spark.stop()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, default=str)
    (args.output_dir / REPORT_FILE_NAME).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 1 if report_failed(report) else 0


if __name__ == "__main__":
    raise SystemExit(main())
