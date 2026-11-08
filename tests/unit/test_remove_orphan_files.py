"""Tests for clearing the files a failed write left in an Iceberg table's folder."""

from __future__ import annotations

import importlib.util
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, mock, skipUnless

from fraudstream.jobs.gold.transactions import _build_spark_session
from fraudstream.jobs.remove_orphan_files import expire_old_snapshots, other_spark_job_running, remove_orphan_files
from fraudstream.jobs.warehouse import IcebergCatalogConfig, WarehouseConfig, write_iceberg_table


class OtherSparkJobRunningTest(TestCase):
    def test_a_temp_folder_besides_our_own_means_a_job_is_running(self) -> None:
        with TemporaryDirectory() as root:
            own_folder = Path(root, "run-own")
            own_folder.mkdir()
            with mock.patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(own_folder)}):
                self.assertFalse(other_spark_job_running())
                Path(root, "run-other").mkdir()
                self.assertTrue(other_spark_job_running())


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class IcebergCleanupTest(TestCase):
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

    def _data_folder(self, table: str) -> Path:
        return Path(self.folder.name, "warehouse", "bronze", table, "data")

    def test_only_files_no_snapshot_references_are_removed(self) -> None:
        write_iceberg_table(self.spark.range(5), "iceberg.bronze.orphans", [], "overwrite")
        orphan = self._data_folder("orphans") / "failed-append.parquet"
        orphan.write_bytes(b"left behind")
        recent = self._data_folder("orphans") / "recent.parquet"
        recent.write_bytes(b"maybe still being written")
        two_days_ago = time.time() - 48 * 3600
        os.utime(orphan, (two_days_ago, two_days_ago))

        listed = remove_orphan_files(self.spark, "iceberg", "bronze.orphans", 24, delete=False)
        self.assertEqual([Path(path).name for path in listed], [orphan.name])
        self.assertTrue(orphan.exists())

        remove_orphan_files(self.spark, "iceberg", "bronze.orphans", 24, delete=True)
        self.assertFalse(orphan.exists())
        self.assertTrue(recent.exists())
        self.assertEqual(self.spark.table("iceberg.bronze.orphans").count(), 5)

        time.sleep(1)
        remove_orphan_files(self.spark, "iceberg", "bronze.orphans", 0, delete=True)
        self.assertFalse(recent.exists())
        self.assertEqual(self.spark.table("iceberg.bronze.orphans").count(), 5)

    def test_expiring_frees_old_copies_and_keeps_the_table(self) -> None:
        for _ in range(3):
            write_iceberg_table(self.spark.range(1000), "iceberg.bronze.overwrites", [], "overwrite")
        parquet_files = lambda: list(self._data_folder("overwrites").glob("*.parquet"))

        report = expire_old_snapshots(self.spark, "iceberg", "bronze.overwrites", delete=False)
        self.assertEqual((report["old_snapshots"], report["data_files_freed"]), (2, 2))
        self.assertEqual(len(parquet_files()), 3)

        expire_old_snapshots(self.spark, "iceberg", "bronze.overwrites", delete=True)
        self.assertEqual(len(parquet_files()), 1)
        self.assertEqual(self.spark.table("iceberg.bronze.overwrites").count(), 1000)
        after = expire_old_snapshots(self.spark, "iceberg", "bronze.overwrites", delete=False)
        self.assertEqual((after["old_snapshots"], after["data_files_freed"]), (0, 0))


if __name__ == "__main__":
    main()
