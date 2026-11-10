"""Tests for compacting, z-ordering and expiring Iceberg tables."""

from __future__ import annotations

import importlib.util
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless
from unittest.mock import patch

from fraudstream.jobs.gold.transactions import _build_spark_session
from fraudstream.jobs.maintenance.iceberg_tables import (
    compact_table,
    expire_old_snapshots,
    maintain,
    parse_zorder,
    report_failed,
    table_file_stats,
    zorder_table,
)
from fraudstream.jobs.warehouse import IcebergCatalogConfig, WarehouseConfig


class ArgumentsTest(TestCase):
    def test_zorder_flag_maps_a_table_to_its_columns(self) -> None:
        self.assertEqual(
            parse_zorder(["iceberg.silver.stg_transactions=customer_id,merchant_id"]),
            {"iceberg.silver.stg_transactions": ["customer_id", "merchant_id"]},
        )

    def test_zorder_flag_without_columns_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            parse_zorder(["iceberg.silver.stg_transactions"])

    def test_the_run_fails_only_when_every_table_failed(self) -> None:
        self.assertFalse(report_failed({"compact": {"a": {"error": None}}, "expire": {"b": {"error": "boom"}}}))
        self.assertTrue(report_failed({"compact": {"a": {"error": "boom"}}, "expire": {"b": {"error": "boom"}}}))


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class IcebergMaintenanceTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.folder = TemporaryDirectory()
        warehouse_uri = f"file://{cls.folder.name}/warehouse"
        cls.spark = _build_spark_session(
            "local[1]",
            warehouse=WarehouseConfig(uri=warehouse_uri),
            iceberg=IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=warehouse_uri),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.folder.cleanup()

    def _append_rows_one_at_a_time(self, table: str, rows: int) -> None:
        self.spark.sql(f"CREATE TABLE {table} (customer_id STRING, merchant_id STRING, amount DOUBLE) USING iceberg")
        for index in range(rows):
            self.spark.createDataFrame(
                [(f"c{index % 3}", f"m{index % 5}", float(index))], "customer_id STRING, merchant_id STRING, amount DOUBLE"
            ).writeTo(table).append()

    def _rows(self, table: str) -> list[tuple]:
        return sorted(tuple(row) for row in self.spark.table(table).collect())

    def test_compaction_merges_small_files_and_keeps_rows(self) -> None:

        table = "iceberg.streaming.compaction"
        self._append_rows_one_at_a_time(table, 20)
        rows_before = self._rows(table)
        self.assertEqual(table_file_stats(self.spark, table)["data_files"], 20)

        compact_table(self.spark, table)

        after = table_file_stats(self.spark, table)
        self.assertEqual((after["data_files"], after["small_files"], after["delete_files"]), (1, 1, 0))
        self.assertEqual(self._rows(table), rows_before)

    def test_compaction_folds_delete_files_into_the_data(self) -> None:
        table = "iceberg.streaming.upserts"
        self.spark.sql(
            f"CREATE TABLE {table} (customer_id STRING, amount DOUBLE) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')"
        )
        self.spark.sql(f"INSERT INTO {table} VALUES ('c0', 0.0), ('c1', 1.0)")
        self.spark.sql(f"INSERT INTO {table} VALUES ('c2', 2.0), ('c3', 3.0)")
        # One row out of each file, so Iceberg writes delete files instead of dropping whole files.
        self.spark.sql(f"DELETE FROM {table} WHERE amount IN (1.0, 2.0)")
        self.assertGreater(table_file_stats(self.spark, table)["delete_files"], 0)

        compact_table(self.spark, table)

        self.assertEqual(table_file_stats(self.spark, table)["delete_files"], 0)
        self.assertEqual(sorted(row.amount for row in self.spark.table(table).collect()), [0.0, 3.0])

    def test_zorder_rewrites_only_the_files_the_filter_matches(self) -> None:
        table = "iceberg.silver.zorder_where"
        self.spark.sql(f"CREATE TABLE {table} (customer_id STRING, event_date DATE) USING iceberg PARTITIONED BY (event_date)")
        for day in ("2026-02-28", "2026-03-01", "2026-03-01"):
            self.spark.sql(f"INSERT INTO {table} VALUES ('c1', DATE '{day}')")

        result = zorder_table(self.spark, table, ["customer_id"], where="event_date >= DATE '2026-03-01'")

        self.assertEqual(result["rewritten_data_files_count"], 2)
        self.assertEqual(self.spark.table(table).count(), 3)

    def test_zorder_writes_small_row_groups_and_restores_the_table_setting(self) -> None:
        table = "iceberg.silver.zorder_row_groups"
        self.spark.sql(f"CREATE TABLE {table} (customer_id STRING, merchant_id STRING) USING iceberg")
        self.spark.range(20_000).selectExpr("uuid() AS customer_id", "uuid() AS merchant_id").coalesce(1).writeTo(table).append()

        with patch("fraudstream.jobs.maintenance.iceberg_tables.ZORDER_ROW_GROUP_BYTES", 65_536):
            zorder_table(self.spark, table, ["customer_id", "merchant_id"])

        # Iceberg records one split offset per Parquet row group.
        [offsets] = [row.split_offsets for row in self.spark.table(f"{table}.files").collect()]
        self.assertGreater(len(offsets), 1)
        properties = {row.key for row in self.spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        self.assertNotIn("write.parquet.row-group-size-bytes", properties)
        self.assertEqual(self.spark.table(table).count(), 20_000)

    def test_zorder_reads_past_the_first_eight_bytes_of_an_id(self) -> None:
        table = "iceberg.silver.zorder_long_ids"
        self.spark.sql(
            f"CREATE TABLE {table} (customer_id STRING, merchant_id STRING) USING iceberg "
            "TBLPROPERTIES ('write.target-file-size-bytes' = '1024')"
        )
        self.spark.range(4_000).selectExpr(
            "format_string('cust_%08d', CAST(rand(1) * 500000 AS INT)) AS customer_id",
            "format_string('merch_%08d', CAST(rand(2) * 50000 AS INT)) AS merchant_id",
        ).coalesce(1).writeTo(table).append()

        zorder_table(self.spark, table, ["customer_id", "merchant_id"])

        # The first 8 bytes, cust_000 to cust_004, split customers into 5 bands of 100,000; each file should hold far less.
        bounds = self.spark.sql(
            f"SELECT readable_metrics.customer_id.lower_bound AS low, readable_metrics.customer_id.upper_bound AS high "
            f"FROM {table}.files"
        ).collect()
        widths = [int(row.high[5:]) - int(row.low[5:]) for row in bounds]
        self.assertGreater(len(widths), 10)
        self.assertLess(sum(widths) / len(widths), 40_000)

    def test_zorder_keeps_every_row(self) -> None:

        table = "iceberg.silver.zorder"
        self._append_rows_one_at_a_time(table, 6)
        rows_before = self._rows(table)

        result = zorder_table(self.spark, table, ["customer_id", "merchant_id"])

        self.assertEqual(result["rewritten_data_files_count"], 6)
        self.assertEqual(self._rows(table), rows_before)

    def test_expire_keeps_the_last_snapshots(self) -> None:

        table = "iceberg.streaming.expiry"
        self._append_rows_one_at_a_time(table, 8)

        expire_old_snapshots(self.spark, table, retain_last=5)

        self.assertEqual(self.spark.table(f"{table}.snapshots").count(), 5)
        self.assertEqual(self.spark.table(table).count(), 8)

    def test_one_failing_table_does_not_stop_the_rest(self) -> None:
        table = "iceberg.gold.feat_healthy"
        self._append_rows_one_at_a_time(table, 3)

        report = maintain(self.spark, compact=["iceberg.gold.feat_missing", table])["compact"]

        self.assertIn("feat_missing", report["iceberg.gold.feat_missing"]["error"])
        self.assertIsNone(report[table]["error"])
        self.assertEqual((report[table]["before"]["data_files"], report[table]["after"]["data_files"]), (3, 1))

    def test_each_action_touches_only_its_own_tables(self) -> None:
        compacted, zordered, expired = "iceberg.streaming.only_compact", "iceberg.silver.only_zorder", "iceberg.streaming.only_expire"
        for table in (compacted, zordered, expired):
            self._append_rows_one_at_a_time(table, 7)

        report = maintain(self.spark, compact=[compacted], zorder={zordered: ["customer_id"]}, expire=[expired])

        self.assertEqual({action: list(tables) for action, tables in report.items()}, {
            "compact": [compacted], "zorder": [zordered], "expire": [expired],
        })
        snapshots = lambda table: self.spark.table(f"{table}.snapshots").count()
        self.assertEqual((table_file_stats(self.spark, compacted)["data_files"], snapshots(compacted)), (1, 8))
        self.assertEqual(snapshots(zordered), 8)
        self.assertEqual((table_file_stats(self.spark, expired)["data_files"], snapshots(expired)), (7, 5))

if __name__ == "__main__":
    main()
