"""Tests for the compact offline and streaming data-quality report."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fraudstream.reports.data_quality import render_html_report


class DataQualityReportTests(unittest.TestCase):
    """Verify that measured evidence is rendered without scanning source rows."""

    def test_renders_offline_metrics_and_silver_dedup_result(self) -> None:
        """Show skew, cardinality, evolution, and before/after dedup evidence."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            offline_dir = root / "offline"
            silver_dir = root / "silver"
            offline_dir.mkdir()
            silver_dir.mkdir()
            config_path = root / "offline_config.json"

            _write_json(
                offline_dir / "_quality_summary.json",
                {
                    "data_format": "partitioned_csv",
                    "written_file_count": 2,
                    "row_count_after_duplicates": 102,
                    "duplicate_row_count": 2,
                    "duplicate_rate_actual": 0.0196,
                    "skew": {
                        "city_distribution_pct": {"New York": 70.0, "Boston": 30.0},
                        "merchant_category_distribution_pct": {"online": 60.0, "fuel": 40.0},
                    },
                    "high_cardinality": {
                        "approx_count_distinct_transaction_id": 100,
                        "approx_count_distinct_customer_id": 80,
                    },
                    "schema_evolution": {
                        "schema_change_date": "2026-04-01",
                        "old_partition_row_count": 50,
                        "new_partition_row_count": 52,
                        "old_partition_missing_columns": ["device_id", "ip_address"],
                    },
                    "late_arrivals": {"late_arrival_rate_actual": 0.04},
                    "raw_quality_issues": {
                        "missing_value_rate_actual": 0.015,
                        "inconsistent_format_rate_actual": 0.01,
                    },
                },
            )
            _write_json(
                silver_dir / "_silver_transactions_summary.json",
                {"output_row_count": 100, "duplicate_rows_removed_count": 2},
            )
            _write_json(
                config_path,
                {
                    "random_seed": 42,
                    "n_transactions": 100,
                    "n_customers": 80,
                    "n_accounts": 90,
                    "n_merchants": 20,
                    "skew_city_ratio": 0.7,
                    "skew_merchant_category_ratio": 0.6,
                    "duplicate_rate": 0.02,
                    "late_arrival_rate": 0.04,
                    "schema_change_date": "2026-04-01",
                },
            )

            report = render_html_report(
                dataset="offline",
                offline_dir=offline_dir,
                silver_dir=silver_dir,
                streaming_dir=root / "unused_stream",
                offline_config=config_path,
                streaming_config=root / "unused_stream_config.json",
                top_n=1,
            )

            self.assertIn("<!doctype html>", report)
            self.assertIn("New York", report)
            self.assertIn("transaction_id", report)
            self.assertIn("50 per evolved column", report)
            self.assertIn("2 / 102 (1.96%)", report)
            self.assertIn("0 retained", report)

    def test_renders_measured_streaming_problem_rates(self) -> None:
        """Show measured burst, late, duplicate, and out-of-order metrics."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stream_dir = root / "stream"
            stream_dir.mkdir()
            config_path = root / "stream_config.json"
            _write_json(
                stream_dir / "_stream_summary.json",
                {
                    "sink_type": "local_jsonl",
                    "topic": "financial_transactions",
                    "base_event_count": 100,
                    "record_count_after_duplicates": 102,
                    "duplicate_record_count": 2,
                    "duplicate_rate_actual": 0.0196,
                    "n_partitions": 4,
                    "stream_problems": {
                        "burst_event_count": 30,
                        "burst_event_rate_actual": 0.2941,
                        "late_event_count": 6,
                        "late_event_rate_actual": 0.0588,
                        "observed_out_of_order_event_count": 8,
                    },
                    "event_time_windows": {
                        "window_minutes": 5,
                        "window_count": 20,
                        "max_records_in_window": 12,
                    },
                },
            )
            _write_json(config_path, _stream_config())

            report = render_html_report(
                dataset="streaming",
                offline_dir=root / "unused_offline",
                silver_dir=root / "unused_silver",
                streaming_dir=stream_dir,
                offline_config=root / "unused_offline_config.json",
                streaming_config=config_path,
                top_n=1,
            )

            self.assertIn("Measured problem rates", report)
            self.assertIn("29.41%", report)
            self.assertIn("5.88%", report)
            self.assertIn("1.96%", report)
            self.assertIn("Observed out-of-order events", report)

    def test_labels_config_as_target_when_stream_summary_is_missing(self) -> None:
        """Avoid presenting configured rates as measured observations."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "stream_config.json"
            _write_json(config_path, _stream_config())

            report = render_html_report(
                dataset="streaming",
                offline_dir=root / "unused_offline",
                silver_dir=root / "unused_silver",
                streaming_dir=root / "missing",
                offline_config=root / "unused_offline_config.json",
                streaming_config=config_path,
                top_n=1,
            )

            self.assertIn("Measured stream evidence is not available", report)
            self.assertIn("Configured targets", report)
            self.assertIn("30.00%", report)


def _stream_config() -> dict[str, object]:
    """Return the minimal streaming configuration used by report tests."""

    return {
        "random_seed": 2026,
        "n_events": 100,
        "n_customers": 80,
        "n_merchants": 20,
        "topic": "financial_transactions",
        "n_partitions": 4,
        "window_minutes": 5,
        "burst_window_count": 2,
        "burst_event_ratio": 0.30,
        "late_event_rate": 0.06,
        "out_of_order_rate": 0.05,
        "duplicate_rate": 0.02,
    }


def _write_json(path: Path, value: object) -> None:
    """Write a small JSON fixture."""

    path.write_text(json.dumps(value), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
