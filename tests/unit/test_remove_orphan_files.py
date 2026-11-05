"""Tests for clearing the files a failed write left in an Iceberg table's folder."""

from __future__ import annotations

import importlib.util
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, mock, skipUnless

from fraudstream.jobs.gold.transactions import _build_spark_session
from fraudstream.jobs.remove_orphan_files import other_spark_job_running, remove_orphan_files
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
class RemoveOrphanFilesTest(TestCase):
    def test_only_files_no_snapshot_references_are_removed(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            warehouse_uri = f"file://{tmp_dir}/warehouse"
            spark = _build_spark_session(
                "local[1]",
                warehouse=WarehouseConfig(uri=warehouse_uri),
                iceberg=IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=warehouse_uri),
            )
            try:
                write_iceberg_table(spark.range(5), "iceberg.bronze.orphans", [], "overwrite")
                data_folder = Path(tmp_dir, "warehouse", "bronze", "orphans", "data")
                orphan = data_folder / "failed-append.parquet"
                orphan.write_bytes(b"left behind")
                recent = data_folder / "recent.parquet"
                recent.write_bytes(b"maybe still being written")
                two_days_ago = time.time() - 48 * 3600
                os.utime(orphan, (two_days_ago, two_days_ago))

                listed = remove_orphan_files(spark, "iceberg", "bronze.orphans", 24, delete=False)
                self.assertEqual([Path(path).name for path in listed], [orphan.name])
                self.assertTrue(orphan.exists())

                remove_orphan_files(spark, "iceberg", "bronze.orphans", 24, delete=True)
                self.assertFalse(orphan.exists())
                self.assertTrue(recent.exists())
                self.assertEqual(spark.table("iceberg.bronze.orphans").count(), 5)

                time.sleep(1)
                remove_orphan_files(spark, "iceberg", "bronze.orphans", 0, delete=True)
                self.assertFalse(recent.exists())
                self.assertEqual(spark.table("iceberg.bronze.orphans").count(), 5)
            finally:
                spark.stop()


if __name__ == "__main__":
    main()
