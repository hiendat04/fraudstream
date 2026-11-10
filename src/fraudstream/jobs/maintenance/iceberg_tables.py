"""Keep Iceberg tables fast to read: merge small files, cluster rows, drop old snapshots.

Each streaming commit and each feature append adds a few small files, and a query
opens every file it can't rule out, so small files slow every read. Compaction
rewrites them into files of about 128 MiB. Z-order rewrites a table so rows with
close values of the given columns share files, which lets a filter on those
columns skip most files. Every rewrite keeps the old files for time travel until
their snapshots expire, so expiry runs last and frees them.

    docker compose exec -w /opt/airflow/project -e MINIO_ACCESS_KEY -e MINIO_SECRET_KEY -e POSTGRES_USER -e POSTGRES_PASSWORD airflow-scheduler python -m fraudstream.jobs.maintenance.iceberg_tables --compact iceberg.streaming.customer_features_5m
    ... --zorder iceberg.silver.stg_transactions=customer_id,merchant_id
    ... --expire iceberg.streaming.customer_features_5m
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from functools import partial
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
ROW_GROUP_PROPERTY = "write.parquet.row-group-size-bytes"
# Small enough that one day of Silver (about 40 MB) splits into several row groups a lookup can skip.
ZORDER_ROW_GROUP_BYTES = 8 * 1024 * 1024
# Enough bytes to take in a whole id, such as merch_00027073.
ZORDER_TEXT_BYTES = 16


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
    """Rewrite the data files (only those `where` matches, if given) so rows close on all of `columns` share files.

    A query skips a Parquet row group whose value range misses its filter, so clustering only pays off when each file
    holds many small row groups. The rewrite uses small ones, then the table's own setting is put back, because the
    unsorted daily writes would only lose compression with them.
    """

    catalog, name = _split(table)
    if where is not None and '"' in where:
        raise ValueError(f"write the filter with single quotes: {where!r}")
    only = f', where => "{where}"' if where else ""
    rewrite = (
        f"{catalog}.system.rewrite_data_files(table => '{name}', strategy => 'sort', "
        f"sort_order => 'zorder({','.join(columns)})', "
        # Iceberg z-orders text by its first 8 bytes by default; every id starts with "cust_00" or "merch_00".
        f"options => map('rewrite-all', 'true', 'var-length-contribution', '{ZORDER_TEXT_BYTES}'){only})"
    )
    previous = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}.get(ROW_GROUP_PROPERTY)
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{ROW_GROUP_PROPERTY}' = '{ZORDER_ROW_GROUP_BYTES}')")
    try:
        return _call(spark, rewrite)
    finally:
        if previous is None:
            spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('{ROW_GROUP_PROPERTY}')")
        else:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{ROW_GROUP_PROPERTY}' = '{previous}')")


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
    compact: Sequence[str] = (),
    zorder: dict[str, list[str]] | None = None,
    expire: Sequence[str] = (),
    zorder_where: str | None = None,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Run each action only on the tables named for it, and record each table's files before and after.

    The actions don't depend on each other. When one run has several, expiry goes last, so it can free the files the
    rewrites just replaced. A failing table doesn't stop the others.
    """

    actions: dict[str, dict[str, Callable[[], dict[str, Any]]]] = {
        "compact": {table: partial(compact_table, spark, table) for table in compact},
        "zorder": {table: partial(zorder_table, spark, table, columns, zorder_where) for table, columns in (zorder or {}).items()},
        "expire": {table: partial(expire_old_snapshots, spark, table) for table in expire},
    }
    report: dict[str, dict[str, dict[str, Any]]] = {}
    for action, runs in actions.items():
        for table, run in runs.items():
            entry: dict[str, Any] = {"before": None, "after": None, "result": None, "error": None}
            report.setdefault(action, {})[table] = entry
            try:
                entry["before"] = table_file_stats(spark, table)
                entry["result"] = run()
                entry["after"] = table_file_stats(spark, table)
            except Exception as error:  # noqa: BLE001 - recorded in the report, and the next table still runs
                entry["error"] = f"{type(error).__name__}: {error}"
    return report


def report_failed(report: dict[str, dict[str, dict[str, Any]]]) -> bool:
    entries = [entry for tables in report.values() for entry in tables.values()]
    return bool(entries) and all(entry["error"] for entry in entries)


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
    parser.add_argument("--compact", action="append", default=[], metavar="TABLE", help="Compact this table, such as iceberg.streaming.x. Repeatable.")
    parser.add_argument("--zorder", action="append", default=[], metavar="TABLE=col1,col2", help="Z-order this table by these columns. Repeatable.")
    parser.add_argument("--expire", action="append", default=[], metavar="TABLE", help="Expire this table's old snapshots. Repeatable.")
    parser.add_argument(
        "--zorder-where", help="Z-order only the files this filter matches, such as \"event_date >= DATE '2026-03-01'\"."
    )
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
    if not (args.compact or zorder or args.expire):
        parser.error("give at least one --compact, --zorder or --expire")

    builder = SparkSession.builder.appName("fraudstream-iceberg-maintenance").master(args.master).config(
        "spark.sql.session.timeZone", "UTC"
    )
    builder = configure_object_storage(builder, warehouse_config_from_args(args))
    builder = configure_iceberg_catalog(builder, iceberg_config_from_args(args), postgres_config_from_args(args))
    spark = apply_spark_conf(configure_spark_builder(builder, retain_ui=0), parse_spark_conf(args.spark_conf)).getOrCreate()
    try:
        report = maintain(spark, args.compact, zorder, args.expire, args.zorder_where)
    finally:
        spark.stop()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, default=str)
    (args.output_dir / REPORT_FILE_NAME).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 1 if report_failed(report) else 0


if __name__ == "__main__":
    raise SystemExit(main())
