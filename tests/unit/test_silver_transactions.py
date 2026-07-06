"""Unit tests for Silver transaction deduplication."""

from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main, skipUnless

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
            v1_file = source_dir / "schema_version=v1" / "transaction_date=2026-01-01" / "transactions.csv"
            v2_file = source_dir / "schema_version=v2" / "transaction_date=2026-04-01" / "transactions.csv"

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

            ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=bronze_dir,
                    ingest_run_id="test_silver_bronze_ingest",
                    ingest_date="2026-07-05",
                    write_mode="overwrite",
                )
            )
            result = build_silver_transactions(
                SilverTransactionsConfig(
                    bronze_dir=bronze_dir,
                    output_dir=silver_dir,
                    write_mode="overwrite",
                )
            )

            self.assertEqual(result.input_row_count, 6)
            self.assertEqual(result.output_row_count, 3)
            self.assertEqual(result.quarantined_row_count, 1)
            self.assertEqual(result.valid_row_count, 2)
            self.assertEqual(result.warning_row_count, 1)
            self.assertEqual(result.duplicate_transaction_id_count, 2)
            self.assertEqual(result.duplicate_rows_removed_count, 2)
            self.assertEqual(result.event_date_count, 2)
            self.assertTrue((silver_dir / "_silver_transactions_summary.json").exists())
            self.assertTrue(any(silver_dir.glob("event_date=2026-01-01/*.parquet")))
            self.assertTrue(any(silver_dir.glob("event_date=2026-04-01/*.parquet")))

            rows = {row["transaction_id"]: row for row in _read_parquet_rows(silver_dir)}
            self.assertEqual(set(rows), {"txn_dup", "txn_tie", "txn_v2_warning"})

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


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a tiny source CSV partition for tests."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_manifest(source_dir: Path, files: list[Path]) -> None:
    """Write a small source manifest for Bronze ingestion."""

    manifest = {
        "dataset": "offline_transactions",
        "source_system": "fraudstream_generator",
        "created_at": "2026-07-05T00:00:00Z",
        "files": [str(path) for path in files],
    }
    with (source_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file)


def _row(
    transaction_id: str,
    amount: str,
    city: str,
    created_ts: str,
    event_timestamp: str = "2026-01-01T12:00:00",
    device_id: str = "",
    ip_address: str = "",
    authentication_method: str = "",
    risk_signal_version: str = "",
) -> dict[str, str]:
    """Return a raw transaction row for Silver tests."""

    return {
        "transaction_id": transaction_id,
        "account_id": f"acct_{transaction_id}",
        "customer_id": f"cust_{transaction_id}",
        "merchant_id": f"merch_{transaction_id}",
        "merchant_category": "online_marketplace",
        "amount": amount,
        "currency": "usd",
        "city": city,
        "channel": "online",
        "transaction_status": "APPROVED",
        "is_fraud": "0",
        "event_timestamp": event_timestamp,
        "created_ts": created_ts,
        "device_id": device_id,
        "ip_address": ip_address,
        "authentication_method": authentication_method,
        "risk_signal_version": risk_signal_version,
    }


def _read_parquet_rows(path: Path):
    """Read a Parquet directory and return rows before stopping Spark."""

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("SilverTransactionsTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        return spark.read.parquet(str(path)).collect()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
