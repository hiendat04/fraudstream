"""Unit tests for Bronze transaction validation."""

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
from fraudstream.jobs.bronze.validate_transactions import (
    BronzeValidationConfig,
    validate_bronze_transactions,
)


@skipUnless(importlib.util.find_spec("pyspark"), "PySpark is not installed")
class BronzeTransactionValidationTest(TestCase):
    """Tests for raw source to Bronze reconciliation."""

    def test_validation_compares_counts_partitions_and_format_issues(self) -> None:
        """Validation should prove Bronze preserved the raw source."""

        with TemporaryDirectory() as tmp_dir:
            root_dir = Path(tmp_dir)
            source_dir = root_dir / "raw_source" / "offline_transactions"
            bronze_dir = root_dir / "bronze" / "raw_transactions"
            report_path = bronze_dir / "_bronze_validation_summary.json"
            v1_file = source_dir / "schema_version=v1" / "transaction_date=2026-01-01" / "transactions.csv"
            v2_file = source_dir / "schema_version=v2" / "transaction_date=2026-04-01" / "transactions.csv"

            _write_csv(v1_file, BASE_COLUMNS, [_padded_v1_row(), _uppercase_v1_row()])
            _write_csv(v2_file, RAW_COLUMNS, [_blank_v2_row()])
            _write_manifest(source_dir, [v1_file, v2_file])

            ingest_transactions_to_bronze(
                BronzeIngestionConfig(
                    source_dir=source_dir,
                    output_dir=bronze_dir,
                    ingest_run_id="test_bronze_validation",
                    ingest_date="2026-07-04",
                    write_mode="overwrite",
                )
            )

            result = validate_bronze_transactions(
                BronzeValidationConfig(
                    source_dir=source_dir,
                    bronze_dir=bronze_dir,
                    report_path=report_path,
                )
            )
            result_payload = result.to_dict()
            format_checks = result_payload["checks"]["raw_format_issue_preservation"]["issues"]

            self.assertTrue(result.passed)
            self.assertTrue(report_path.exists())
            self.assertEqual(result_payload["source"]["row_count"], 3)
            self.assertEqual(result_payload["bronze"]["row_count"], 3)
            self.assertEqual(result_payload["source"]["csv_file_count"], 2)
            self.assertEqual(result_payload["bronze"]["distinct_source_file_count"], 2)
            self.assertEqual(result_payload["source"]["partition_count"], 2)
            self.assertEqual(result_payload["bronze"]["distinct_schema_date_partition_count"], 2)
            self.assertEqual(result_payload["bronze"]["partition_count"], 2)
            self.assertEqual(format_checks["padded_city_rows"]["expected"], 1)
            self.assertEqual(format_checks["uppercase_city_rows"]["expected"], 1)
            self.assertEqual(format_checks["lowercase_currency_rows"]["expected"], 2)
            self.assertEqual(format_checks["uppercase_status_rows"]["expected"], 2)
            self.assertEqual(format_checks["blank_city_rows"]["expected"], 1)
            self.assertEqual(format_checks["blank_merchant_id_rows"]["expected"], 1)
            self.assertEqual(format_checks["v1_missing_device_id_rows"]["expected"], 2)
            self.assertEqual(format_checks["v2_blank_device_id_rows"]["expected"], 1)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a tiny source CSV partition for tests."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_manifest(source_dir: Path, files: list[Path]) -> None:
    """Write a small source manifest for file discovery."""

    manifest = {
        "dataset": "offline_transactions",
        "source_system": "fraudstream_generator",
        "created_at": "2026-07-04T00:00:00Z",
        "files": [str(path) for path in files],
    }
    with (source_dir / "_manifest.json").open("w", encoding="utf-8") as file:
        json.dump(manifest, file)


def _base_row() -> dict[str, str]:
    """Return a minimal transaction row for validation tests."""

    return {
        "transaction_id": "txn_test_000001",
        "account_id": "acct_test_000001",
        "customer_id": "cust_test_000001",
        "merchant_id": "merch_test_000001",
        "merchant_category": "online_marketplace",
        "amount": "42.50",
        "currency": "USD",
        "city": "Detroit",
        "channel": "online",
        "transaction_status": "approved",
        "is_fraud": "0",
        "event_timestamp": "2026-01-01T12:00:00",
        "created_ts": "2026-01-01T12:05:00",
    }


def _padded_v1_row() -> dict[str, str]:
    """Return a v1 row with padding, lowercase currency, and blank merchant."""

    row = _base_row()
    row.update(
        {
            "city": " New York ",
            "currency": "usd",
            "merchant_id": "",
            "transaction_status": "APPROVED",
        }
    )
    return row


def _uppercase_v1_row() -> dict[str, str]:
    """Return a v1 row with uppercase city."""

    row = _base_row()
    row.update(
        {
            "transaction_id": "txn_test_000002",
            "city": "DETROIT",
        }
    )
    return row


def _blank_v2_row() -> dict[str, str]:
    """Return a v2 row with blank city and blank evolved device field."""

    row = _base_row()
    row.update(
        {
            "transaction_id": "txn_test_000003",
            "currency": "usd",
            "city": "",
            "transaction_status": "DECLINED",
            "device_id": "",
            "ip_address": "10.1.2.3",
            "authentication_method": "otp",
            "risk_signal_version": "v2",
        }
    )
    return row


if __name__ == "__main__":
    main()
