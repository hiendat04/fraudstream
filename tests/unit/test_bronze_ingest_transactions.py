"""Unit tests for Bronze transaction ingestion."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

from fraudstream import storage
from fraudstream.jobs.bronze.ingest_transactions import (
    BASE_COLUMNS,
    RAW_COLUMNS,
    BronzeIngestionConfig,
    ingest_transactions_to_bronze,
)
from fraudstream.jobs.warehouse import IcebergCatalogConfig, WarehouseConfig


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class BronzeTransactionIngestionTest(TestCase):
    """Tests for CSV to Bronze Parquet ingestion."""

    def test_ingestion_preserves_raw_rows_and_adds_metadata(self) -> None:
        """Bronze ingestion should preserve source behavior and write metadata."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            output_dir = root_dir / "bronze" / "raw_transactions"
            v1_file = f"file://{source_dir}/schema_version=v1/transaction_date=2026-01-01/transactions.csv"
            v2_file = f"file://{source_dir}/schema_version=v2/transaction_date=2026-04-01/transactions.csv"

            _write_csv(v1_file, BASE_COLUMNS, [_v1_row(), _v1_row()])
            _write_csv(v2_file, RAW_COLUMNS, [_v2_row()])
            _write_manifest(source_dir, [v1_file, v2_file])

            warehouse_dir = root_dir / "warehouse"
            iceberg_uri = f"file://{warehouse_dir}/iceberg"
            result = ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=output_dir,
                    ingest_run_id="test_bronze_ingest",
                    ingest_date="2026-07-04",
                    write_mode="overwrite",
                    warehouse=WarehouseConfig(uri=f"file://{warehouse_dir}"),
                    iceberg=IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=iceberg_uri),
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.row_count, 3)
            self.assertEqual(result.source_file_count, 2)
            self.assertEqual(result.duplicate_transaction_id_count, 1)
            self.assertEqual(result.schema_versions, ("v1", "v2"))
            self.assertEqual(result.transaction_date_count, 2)
            self.assertTrue((output_dir / "_bronze_ingestion_summary.json").exists())

            rows = _read_iceberg_rows("iceberg.bronze.raw_transactions", iceberg_uri)
            self.assertEqual(len(rows), 3)

            rows_by_version = {
                row["schema_version"]: row
                for row in sorted(rows, key=lambda item: item["schema_version"])
                if row["schema_version"] in {"v1", "v2"}
            }
            v1_row = rows_by_version["v1"]
            v2_row = rows_by_version["v2"]

            self.assertEqual(v1_row["transaction_id"], "txn_test_000001")
            self.assertEqual(v1_row["city"], " New York ")
            self.assertEqual(v1_row["currency"], "usd")
            self.assertEqual(v1_row["transaction_status"], "APPROVED")
            self.assertIsNone(v1_row["device_id"])
            self.assertEqual(v1_row["_source_system"], "fraudstream_generator")
            self.assertEqual(v1_row["_source_dataset"], "offline_transactions")
            self.assertEqual(v1_row["_ingest_run_id"], "test_bronze_ingest")
            self.assertEqual(len(v1_row["_raw_record_hash"]), 64)
            self.assertGreaterEqual(v1_row["_source_row_number"], 1)
            self.assertIn("transactions.csv", v1_row["_source_file_path"])

            self.assertEqual(v2_row["transaction_id"], "txn_test_000002")
            self.assertEqual(v2_row["device_id"], "")
            self.assertEqual(v2_row["ip_address"], "10.1.2.3")
            self.assertEqual(v2_row["authentication_method"], "otp")
            self.assertEqual(v2_row["risk_signal_version"], "v2")

    def test_ingestion_allows_missing_evolved_columns(self) -> None:
        """Bronze should read old and partial evolved schemas into one table."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            output_dir = root_dir / "bronze" / "raw_transactions"
            v1_file = f"file://{source_dir}/schema_version=v1/transaction_date=2026-01-01/transactions.csv"
            v2_file = f"file://{source_dir}/schema_version=v2/transaction_date=2026-04-01/transactions.csv"
            partial_v2_columns = [*BASE_COLUMNS, "ip_address"]

            _write_csv(v1_file, BASE_COLUMNS, [_v1_row()])
            _write_csv(v2_file, partial_v2_columns, [_v2_row()])
            _write_manifest(source_dir, [v1_file, v2_file])

            warehouse_dir = root_dir / "warehouse"
            iceberg_uri = f"file://{warehouse_dir}/iceberg"
            result = ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=output_dir,
                    ingest_run_id="test_schema_evolution",
                    ingest_date="2026-07-04",
                    write_mode="overwrite",
                    warehouse=WarehouseConfig(uri=f"file://{warehouse_dir}"),
                    iceberg=IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=iceberg_uri),
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.row_count, 2)
            self.assertEqual(result.schema_versions, ("v1", "v2"))

            rows = _read_iceberg_rows("iceberg.bronze.raw_transactions", iceberg_uri)
            rows_by_version = {row["schema_version"]: row for row in rows}

            self.assertIsNone(rows_by_version["v1"]["device_id"])
            self.assertIsNone(rows_by_version["v1"]["ip_address"])
            self.assertIsNone(rows_by_version["v1"]["authentication_method"])
            self.assertIsNone(rows_by_version["v1"]["risk_signal_version"])

            self.assertIsNone(rows_by_version["v2"]["device_id"])
            self.assertEqual(rows_by_version["v2"]["ip_address"], "10.1.2.3")
            self.assertIsNone(rows_by_version["v2"]["authentication_method"])
            self.assertIsNone(rows_by_version["v2"]["risk_signal_version"])


def _write_csv(uri: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a tiny source CSV partition for tests, via the same `storage`
    module the generator uses (a local `file://` URI stands in for MinIO)."""

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    storage.put_text(WarehouseConfig(), uri, buffer.getvalue())


def _write_manifest(source_dir: Path, files: list[str]) -> None:
    """Write a small source manifest for file discovery."""

    manifest = {
        "dataset": "offline_transactions",
        "source_system": "fraudstream_generator",
        "created_at": "2026-07-04T00:00:00Z",
        "files": files,
    }
    source_dir.mkdir(parents=True, exist_ok=True)
    with (source_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file)


def _v1_row() -> dict[str, str]:
    """Return a raw v1 row with deliberate duplicate and format issues."""

    return {
        "transaction_id": "txn_test_000001",
        "account_id": "acct_test_000001",
        "customer_id": "cust_test_000001",
        "merchant_id": "merch_test_000001",
        "merchant_category": "online_marketplace",
        "amount": "42.50",
        "currency": "usd",
        "city": " New York ",
        "channel": "online",
        "transaction_status": "APPROVED",
        "is_fraud": "0",
        "event_timestamp": "2026-01-01T12:00:00",
        "created_ts": "2026-01-01T12:05:00",
    }


def _v2_row() -> dict[str, str]:
    """Return a raw v2 row with evolved fields."""

    return {
        "transaction_id": "txn_test_000002",
        "account_id": "acct_test_000002",
        "customer_id": "cust_test_000002",
        "merchant_id": "merch_test_000002",
        "merchant_category": "electronics",
        "amount": "199.99",
        "currency": "USD",
        "city": "Detroit",
        "channel": "mobile_wallet",
        "transaction_status": "approved",
        "is_fraud": "1",
        "event_timestamp": "2026-04-01T08:30:00",
        "created_ts": "2026-04-01T11:00:00",
        "device_id": "",
        "ip_address": "10.1.2.3",
        "authentication_method": "otp",
        "risk_signal_version": "v2",
    }


def _read_iceberg_rows(table: str, iceberg_warehouse_uri: str):
    """Read an Iceberg table through a fresh Hadoop-catalog session and return its rows.

    `catalog_type="hadoop"` stores catalog metadata directly under the local
    `file://` warehouse path instead of PostgreSQL, so this reads back
    exactly what the job under test wrote without needing a running database.
    """

    from pyspark.sql import SparkSession

    from fraudstream.jobs.warehouse import IcebergCatalogConfig, PostgresJdbcConfig, configure_iceberg_catalog

    builder = (
        SparkSession.builder.appName("BronzeTransactionIngestionTest")
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


if __name__ == "__main__":
    main()
