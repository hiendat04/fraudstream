"""Unit tests for Silver transaction deduplication."""

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
from fraudstream.jobs.silver.transactions import (
    SilverTransactionsConfig,
    build_silver_transactions,
)
from fraudstream.jobs.warehouse import IcebergCatalogConfig, WarehouseConfig


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class SilverTransactionsTest(TestCase):
    """Tests for Bronze to Silver transaction processing."""

    def test_deduplicates_with_deterministic_record_selection(self) -> None:
        """Silver should select one clean row per transaction_id deterministically."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            bronze_dir = root_dir / "bronze" / "raw_transactions"
            silver_dir = root_dir / "silver" / "transactions"
            v1_file = f"file://{source_dir}/schema_version=v1/transaction_date=2026-01-01/transactions.csv"
            v2_file = f"file://{source_dir}/schema_version=v2/transaction_date=2026-04-01/transactions.csv"

            _write_csv(
                v1_file,
                BASE_COLUMNS,
                [
                    _row(
                        transaction_id="txn_dup",
                        amount="10.50",
                        city=" New York ",
                        created_ts="2026-01-01T12:02:00",
                    ),
                    _row(
                        transaction_id="txn_dup",
                        amount="11.00",
                        city="DETROIT",
                        created_ts="2026-01-01T12:10:00",
                    ),
                    _row(
                        transaction_id="txn_tie",
                        amount="21.00",
                        city="Chicago",
                        event_timestamp="2026-01-01T13:00:00",
                        created_ts="2026-01-01T13:10:00",
                    ),
                    _row(
                        transaction_id="txn_tie",
                        amount="22.00",
                        city="Chicago",
                        event_timestamp="2026-01-01T13:00:00",
                        created_ts="2026-01-01T13:10:00",
                    ),
                    _row(
                        transaction_id="txn_bad",
                        amount="not-a-number",
                        city="Miami",
                        created_ts="2026-01-01T14:10:00",
                    ),
                ],
            )
            _write_csv(
                v2_file,
                RAW_COLUMNS,
                [
                    _row(
                        transaction_id="txn_v2_warning",
                        amount="30.00",
                        city="Seattle",
                        event_timestamp="2026-04-01T08:00:00",
                        created_ts="2026-04-01T08:05:00",
                        device_id="",
                        ip_address="",
                        authentication_method="OTP",
                        risk_signal_version="v2",
                    )
                ],
            )
            _write_manifest(source_dir, [v1_file, v2_file])

            warehouse_dir = root_dir / "warehouse"
            warehouse = WarehouseConfig(uri=f"file://{warehouse_dir}")
            iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=f"file://{warehouse_dir}/iceberg")

            ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=bronze_dir,
                    ingest_run_id="test_silver_bronze_ingest",
                    ingest_date="2026-07-05",
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )
            result = build_silver_transactions(
                SilverTransactionsConfig(
                    output_dir=silver_dir,
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.input_row_count, 6)
            self.assertEqual(result.output_row_count, 3)
            self.assertEqual(result.quarantined_row_count, 1)
            self.assertEqual(result.valid_row_count, 2)
            self.assertEqual(result.warning_row_count, 1)
            self.assertEqual(result.duplicate_transaction_id_count, 2)
            self.assertEqual(result.duplicate_rows_removed_count, 2)
            self.assertEqual(result.quality_issue_row_count, 4)
            self.assertEqual(result.event_date_count, 2)
            self.assertTrue((silver_dir / "_silver_transactions_summary.json").exists())
            self.assertTrue((silver_dir / "_silver_quality_report.json").exists())

            rows = {
                row["transaction_id"]: row
                for row in _read_iceberg_rows("iceberg.silver.stg_transactions", iceberg.warehouse_uri)
            }
            self.assertEqual(set(rows), {"txn_dup", "txn_tie", "txn_v2_warning"})
            self.assertEqual({str(row["event_date"]) for row in rows.values()}, {"2026-01-01", "2026-04-01"})

            self.assertEqual(str(rows["txn_dup"]["amount"]), "11.00")
            self.assertEqual(rows["txn_dup"]["city"], "Detroit")
            self.assertEqual(rows["txn_dup"]["currency"], "USD")
            self.assertEqual(rows["txn_dup"]["transaction_status"], "approved")
            self.assertEqual(rows["txn_dup"]["duplicate_record_count"], 2)
            self.assertEqual(rows["txn_dup"]["dedup_rank"], 1)
            self.assertEqual(rows["txn_dup"]["quality_status"], "valid")

            self.assertEqual(str(rows["txn_tie"]["amount"]), "22.00")
            self.assertEqual(rows["txn_tie"]["duplicate_record_count"], 2)
            self.assertEqual(rows["txn_tie"]["dedup_rank"], 1)

            self.assertEqual(rows["txn_v2_warning"]["quality_status"], "warning")
            self.assertIn("missing_evolved_value", rows["txn_v2_warning"]["quality_issue_codes"])
            self.assertEqual(rows["txn_v2_warning"]["authentication_method"], "otp")
            self.assertEqual(str(rows["txn_v2_warning"]["event_date"]), "2026-04-01")

            quality_rows = _read_iceberg_rows("iceberg.silver.stg_transaction_quality_issues", iceberg.warehouse_uri)
            action_counts = _count_rows_by(quality_rows, "_silver_record_action")
            self.assertEqual(action_counts, {"duplicate_rejected": 2, "quarantined": 1, "selected": 1})

            report = _read_json(silver_dir / "_silver_quality_report.json")
            self.assertEqual(report["row_counts"]["quality_evidence"], 4)
            self.assertEqual(report["quality_issue_counts"]["invalid_amount"], 1)
            self.assertEqual(report["quality_issue_counts"]["missing_evolved_value"], 1)
            self.assertEqual(report["record_action_counts"]["duplicate_rejected"], 2)

    def test_standardizes_injected_raw_format_issues(self) -> None:
        """Silver should normalize easy-to-clean raw string format problems."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            bronze_dir = root_dir / "bronze" / "raw_transactions"
            silver_dir = root_dir / "silver" / "transactions"
            source_file = f"file://{source_dir}/schema_version=v2/transaction_date=2026-04-01/transactions.csv"

            _write_csv(
                source_file,
                RAW_COLUMNS,
                [
                    _row(
                        transaction_id="txn_format",
                        amount=" 1,234.50 ",
                        city="  san   FRANCISCO  ",
                        created_ts="2026-04-01T08:05:00",
                        event_timestamp="2026-04-01T08:00:00",
                        merchant_category=" Online Marketplace ",
                        currency=" usd ",
                        channel=" Mobile Wallet ",
                        transaction_status=" APPROVED ",
                        device_id=" dev_format_001 ",
                        ip_address=" 10.0.0.1 ",
                        authentication_method=" OTP ",
                        risk_signal_version=" V2 ",
                    )
                ],
            )
            _write_manifest(source_dir, [source_file])

            warehouse_dir = root_dir / "warehouse"
            warehouse = WarehouseConfig(uri=f"file://{warehouse_dir}")
            iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=f"file://{warehouse_dir}/iceberg")

            ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=bronze_dir,
                    ingest_run_id="test_silver_format_cleanup",
                    ingest_date="2026-07-05",
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )
            result = build_silver_transactions(
                SilverTransactionsConfig(
                    output_dir=silver_dir,
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.input_row_count, 1)
            self.assertEqual(result.output_row_count, 1)
            self.assertEqual(result.valid_row_count, 1)
            self.assertEqual(result.warning_row_count, 0)
            self.assertEqual(result.quarantined_row_count, 0)

            row = _read_iceberg_rows("iceberg.silver.stg_transactions", iceberg.warehouse_uri)[0]
            self.assertEqual(row["merchant_category"], "online_marketplace")
            self.assertEqual(str(row["amount"]), "1234.50")
            self.assertEqual(row["currency"], "USD")
            self.assertEqual(row["city"], "San Francisco")
            self.assertEqual(row["channel"], "mobile_wallet")
            self.assertEqual(row["transaction_status"], "approved")
            self.assertEqual(row["device_id"], "dev_format_001")
            self.assertEqual(row["ip_address"], "10.0.0.1")
            self.assertEqual(row["authentication_method"], "otp")
            self.assertEqual(row["risk_signal_version"], "v2")
            self.assertEqual(row["quality_status"], "valid")

    def test_quarantines_missing_required_keys_without_silent_drop(self) -> None:
        """Rows missing required keys should be reported instead of disappearing."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            bronze_dir = root_dir / "bronze" / "raw_transactions"
            silver_dir = root_dir / "silver" / "transactions"
            source_file = f"file://{source_dir}/schema_version=v1/transaction_date=2026-01-01/transactions.csv"

            _write_csv(
                source_file,
                BASE_COLUMNS,
                [
                    _row(
                        transaction_id="txn_nullable_ok",
                        amount="42.00",
                        city="",
                        created_ts="2026-01-01T12:05:00",
                        merchant_id="",
                        merchant_category="",
                    ),
                    _row(
                        transaction_id="txn_missing_keys",
                        account_id="",
                        customer_id="",
                        amount="19.00",
                        city="Boston",
                        created_ts="2026-01-01T13:05:00",
                    ),
                ],
            )
            _write_manifest(source_dir, [source_file])

            warehouse_dir = root_dir / "warehouse"
            warehouse = WarehouseConfig(uri=f"file://{warehouse_dir}")
            iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=f"file://{warehouse_dir}/iceberg")

            ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=bronze_dir,
                    ingest_run_id="test_silver_missing_values",
                    ingest_date="2026-07-05",
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )
            result = build_silver_transactions(
                SilverTransactionsConfig(
                    output_dir=silver_dir,
                    write_mode="overwrite",
                    warehouse=warehouse,
                    iceberg=iceberg,
                    write_to_postgres=False,
                )
            )

            self.assertEqual(result.input_row_count, 2)
            self.assertEqual(result.output_row_count, 1)
            self.assertEqual(result.valid_row_count, 1)
            self.assertEqual(result.quarantined_row_count, 1)
            self.assertEqual(result.quality_issue_row_count, 1)

            silver_row = _read_iceberg_rows("iceberg.silver.stg_transactions", iceberg.warehouse_uri)[0]
            self.assertEqual(silver_row["transaction_id"], "txn_nullable_ok")
            self.assertIsNone(silver_row["merchant_id"])
            self.assertIsNone(silver_row["merchant_category"])
            self.assertIsNone(silver_row["city"])
            self.assertEqual(silver_row["quality_status"], "valid")

            quality_rows = _read_iceberg_rows("iceberg.silver.stg_transaction_quality_issues", iceberg.warehouse_uri)
            self.assertEqual(len(quality_rows), 1)
            self.assertEqual(quality_rows[0]["transaction_id"], "txn_missing_keys")
            self.assertEqual(quality_rows[0]["_silver_record_action"], "quarantined")
            self.assertIn("missing_account_id", quality_rows[0]["quality_issue_codes"])
            self.assertIn("missing_customer_id", quality_rows[0]["quality_issue_codes"])

            report = _read_json(silver_dir / "_silver_quality_report.json")
            self.assertEqual(report["quality_issue_counts"]["missing_account_id"], 1)
            self.assertEqual(report["quality_issue_counts"]["missing_customer_id"], 1)
            self.assertEqual(report["nullable_field_missing_counts"]["city"], 1)
            self.assertEqual(
                {rule["field"] for rule in report["required_transaction_key_rules"]},
                {"transaction_id", "account_id", "customer_id"},
            )


def _write_csv(uri: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a tiny source CSV partition for tests, via the same `storage`
    module the generator uses (a local `file://` URI stands in for MinIO)."""

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    storage.put_text(WarehouseConfig(), uri, buffer.getvalue())


def _write_manifest(source_dir: Path, files: list[str]) -> None:
    """Write a small source manifest for Bronze ingestion."""

    manifest = {
        "dataset": "offline_transactions",
        "source_system": "fraudstream_generator",
        "created_at": "2026-07-05T00:00:00Z",
        "files": files,
    }
    source_dir.mkdir(parents=True, exist_ok=True)
    with (source_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file)


def _row(
    transaction_id: str,
    amount: str,
    city: str,
    created_ts: str,
    account_id: str | None = None,
    customer_id: str | None = None,
    merchant_id: str | None = None,
    event_timestamp: str = "2026-01-01T12:00:00",
    merchant_category: str = "online_marketplace",
    currency: str = "usd",
    channel: str = "online",
    transaction_status: str = "APPROVED",
    device_id: str = "",
    ip_address: str = "",
    authentication_method: str = "",
    risk_signal_version: str = "",
) -> dict[str, str]:
    """Return a raw transaction row for Silver tests."""

    return {
        "transaction_id": transaction_id,
        "account_id": account_id if account_id is not None else f"acct_{transaction_id}",
        "customer_id": customer_id if customer_id is not None else f"cust_{transaction_id}",
        "merchant_id": merchant_id if merchant_id is not None else f"merch_{transaction_id}",
        "merchant_category": merchant_category,
        "amount": amount,
        "currency": currency,
        "city": city,
        "channel": channel,
        "transaction_status": transaction_status,
        "is_fraud": "0",
        "event_timestamp": event_timestamp,
        "created_ts": created_ts,
        "device_id": device_id,
        "ip_address": ip_address,
        "authentication_method": authentication_method,
        "risk_signal_version": risk_signal_version,
    }


def _read_json(path: Path) -> dict:
    """Read a JSON artifact from a test output directory."""

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _count_rows_by(rows, column_name: str) -> dict[str, int]:
    """Return simple counts by one Row field."""

    counts: dict[str, int] = {}
    for row in rows:
        counts[row[column_name]] = counts.get(row[column_name], 0) + 1
    return counts


def _read_iceberg_rows(table: str, iceberg_warehouse_uri: str):
    """Read an Iceberg table through a fresh Hadoop-catalog session and return its rows."""

    from pyspark.sql import SparkSession

    from fraudstream.jobs.warehouse import PostgresJdbcConfig, configure_iceberg_catalog

    builder = (
        SparkSession.builder.appName("SilverTransactionsTest")
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
