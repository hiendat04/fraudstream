"""Unit tests for the transaction-labels Gold load."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.gold.transaction_labels import (
    TransactionLabelsConfig,
    build_transaction_labels,
)
from fraudstream.jobs.warehouse import IcebergCatalogConfig, PostgresJdbcConfig, WarehouseConfig, configure_iceberg_catalog


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class TransactionLabelsTest(TestCase):
    """Tests for loading the raw label CSV into `gold.transaction_labels`."""

    def test_loads_label_csv_into_gold_table(self) -> None:
        """The build should record accurate counts and write readable Iceberg rows."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            gold_dir = root_dir / "gold"
            warehouse_dir = root_dir / "warehouse"
            labels_dir = root_dir / "labels"
            iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=f"file://{warehouse_dir}/iceberg")
            _write_label_fixture(
                labels_dir,
                [
                    ("txn_001", "0", "2026-01-05T13:22:41"),
                    ("txn_002", "1", "2026-01-05T14:05:09"),
                    ("txn_003", "0", "2026-01-06T08:00:00"),
                    ("txn_004", "1", "2026-01-06T09:30:15"),
                ],
            )

            result = build_transaction_labels(
                TransactionLabelsConfig(
                    labels_uri=f"file://{labels_dir}",
                    gold_dir=gold_dir,
                    write_mode="overwrite",
                    processed_at=None,
                    warehouse=WarehouseConfig(uri=f"file://{warehouse_dir}"),
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.row_count, 4)
            self.assertEqual(result.distinct_transaction_id_count, 4)
            self.assertEqual(result.fraud_row_count, 2)
            self.assertEqual(result.iceberg_table, "iceberg.gold.transaction_labels")

            summary_path = gold_dir / "_transaction_labels_summary.json"
            self.assertTrue(summary_path.exists())
            summary = _read_json(summary_path)
            self.assertEqual(summary["row_count"], 4)
            self.assertEqual(summary["distinct_transaction_id_count"], 4)
            self.assertEqual(summary["fraud_row_count"], 2)

            rows = {row["transaction_id"]: row["is_fraud"] for row in _read_iceberg_rows(
                "iceberg.gold.transaction_labels", iceberg.warehouse_uri
            )}
            self.assertEqual(
                rows,
                {"txn_001": 0, "txn_002": 1, "txn_003": 0, "txn_004": 1},
            )

    def test_empty_label_csv_raises(self) -> None:
        """An empty label CSV should raise instead of writing an empty label table."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            warehouse_dir = root_dir / "warehouse"
            labels_dir = root_dir / "labels"
            iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=f"file://{warehouse_dir}/iceberg")
            _write_label_fixture(labels_dir, [])

            with self.assertRaises(RuntimeError):
                build_transaction_labels(
                    TransactionLabelsConfig(
                        labels_uri=f"file://{labels_dir}",
                        gold_dir=root_dir / "gold",
                        write_mode="overwrite",
                        warehouse=WarehouseConfig(uri=f"file://{warehouse_dir}"),
                        iceberg=iceberg,
                        write_to_postgres=False,
                    )
                )


def _write_label_fixture(labels_dir: Path, rows: list[tuple[str, str, str]]) -> None:
    """Write a fixture `transaction_labels.csv` with the generator's exact header."""

    labels_dir.mkdir(parents=True, exist_ok=True)
    lines = ["transaction_id,is_fraud,event_timestamp"]
    lines.extend(f"{transaction_id},{is_fraud},{event_timestamp}" for transaction_id, is_fraud, event_timestamp in rows)
    (labels_dir / "transaction_labels.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_iceberg_rows(table: str, iceberg_warehouse_uri: str):
    """Read an Iceberg table through a fresh Hadoop-catalog session and return its rows."""

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("TransactionLabelsTestReader")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    builder = configure_iceberg_catalog(
        builder,
        IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=iceberg_warehouse_uri),
        PostgresJdbcConfig(),
    )
    spark = builder.getOrCreate()
    try:
        return spark.table(table).collect()
    finally:
        spark.stop()


def _read_json(path: Path) -> dict:
    """Read a JSON artifact."""

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


if __name__ == "__main__":
    main()
