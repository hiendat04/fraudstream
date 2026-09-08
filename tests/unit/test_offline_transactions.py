"""Unit tests for the offline transaction generator."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from fraudstream.generators.offline_transactions import OfflineGeneratorConfig, generate_offline_transactions


class OfflineTransactionGeneratorTest(TestCase):
    """Tests for offline data problem simulation."""

    def test_generator_simulates_required_offline_problems(self):
        """The generator should emit evidence for realistic, solvable raw-data problems."""

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "offline_transactions"
            raw_dir = Path(tmp_dir) / "raw"
            config = OfflineGeneratorConfig(
                random_seed=7,
                n_transactions=500,
                n_customers=450,
                n_accounts=470,
                n_merchants=260,
                start_date=date(2026, 1, 1),
                days_history=20,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.70,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.60,
                duplicate_rate=0.02,
                late_arrival_rate=0.05,
                missing_value_rate=0.03,
                inconsistent_format_rate=0.03,
                burst_day_count=3,
                fraud_ring_count=3,
                schema_change_date=date(2026, 1, 10),
                output_dir=output_dir,
                raw_uri=f"file://{raw_dir}",
                labels_uri=f"file://{Path(tmp_dir) / 'labels'}",
            )

            summary = generate_offline_transactions(config)

            self.assertEqual(summary["duplicate_row_count"], 10)
            self.assertGreater(summary["skew"]["city_distribution_pct"]["New York"], 60)
            self.assertGreater(summary["skew"]["merchant_category_distribution_pct"]["online_marketplace"], 50)
            self.assertGreater(summary["fraud"]["fraud_row_count"], 0)
            self.assertGreater(summary["traffic_patterns"]["burst_row_count"], 0)
            self.assertEqual(len(summary["traffic_patterns"]["burst_dates"]), 3)
            self.assertGreater(summary["late_arrivals"]["late_arrival_row_count"], 0)
            self.assertGreater(summary["raw_quality_issues"]["missing_value_row_count"], 0)
            self.assertGreater(summary["raw_quality_issues"]["inconsistent_format_row_count"], 0)
            self.assertEqual(summary["high_cardinality"]["approx_count_distinct_transaction_id"], 500)
            self.assertGreater(summary["schema_evolution"]["old_partition_row_count"], 0)
            self.assertGreater(summary["schema_evolution"]["new_partition_row_count"], 0)
            self.assertTrue((config.output_dir / "_manifest.json").exists())
            self.assertTrue((config.output_dir / "_quality_summary.json").exists())
            self.assertTrue(any(raw_dir.glob("schema_version=v1/transaction_date=*/transactions.csv")))
            self.assertTrue(any(raw_dir.glob("schema_version=v2/transaction_date=*/transactions.csv")))

    def test_generator_writes_label_table(self):
        """The generator writes a separate (id, label, event_timestamp) table."""

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "offline_transactions"
            raw_dir = Path(tmp_dir) / "raw"
            labels_dir = Path(tmp_dir) / "labels"
            config = OfflineGeneratorConfig(
                random_seed=7,
                n_transactions=300,
                n_customers=200,
                n_accounts=210,
                n_merchants=100,
                start_date=date(2026, 1, 1),
                days_history=20,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.70,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.60,
                duplicate_rate=0.05,
                schema_change_date=date(2026, 1, 10),
                output_dir=output_dir,
                raw_uri=f"file://{raw_dir}",
                labels_uri=f"file://{labels_dir}",
            )

            summary = generate_offline_transactions(config)

            label_path = labels_dir / "transaction_labels.csv"
            self.assertTrue(label_path.exists())

            with label_path.open() as f:
                reader = csv.reader(f)
                header = next(reader)
                data_rows = list(reader)

            self.assertEqual(header, ["id", "label", "event_timestamp"])
            self.assertEqual(len(data_rows), 300)
            self.assertTrue(all(label in {"0", "1"} for _id, label, _ts in data_rows))
            self.assertTrue(any(label == "1" for _id, label, _ts in data_rows))

            self.assertEqual(summary["label_table"]["row_count"], 300)
            self.assertEqual(summary["label_table"]["columns"], ["id", "label", "event_timestamp"])
            self.assertEqual(summary["label_table"]["uri"], f"file://{labels_dir}/transaction_labels.csv")

            with (output_dir / "_manifest.json").open() as f:
                manifest = json.load(f)
            self.assertEqual(manifest["label_file"], summary["label_table"]["uri"])

    def test_drift_disabled_by_default(self):
        """With no drift_start_date configured, the drift section reports disabled."""

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "offline_transactions"
            raw_dir = Path(tmp_dir) / "raw"
            config = OfflineGeneratorConfig(
                random_seed=7,
                n_transactions=200,
                n_customers=150,
                n_accounts=160,
                n_merchants=80,
                start_date=date(2026, 1, 1),
                days_history=20,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.70,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.60,
                duplicate_rate=0.0,
                schema_change_date=date(2026, 1, 10),
                output_dir=output_dir,
                raw_uri=f"file://{raw_dir}",
                labels_uri=f"file://{Path(tmp_dir) / 'labels'}",
            )

            summary = generate_offline_transactions(config)

            self.assertEqual(summary["drift"], {"enabled": False})

    def test_drift_ramps_amount_upward_after_drift_start_date(self):
        """Configured drift measurably raises mean amount after drift_start_date."""

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "offline_transactions"
            raw_dir = Path(tmp_dir) / "raw"
            config = OfflineGeneratorConfig(
                random_seed=7,
                n_transactions=4000,
                n_customers=1500,
                n_accounts=1600,
                n_merchants=400,
                start_date=date(2026, 1, 1),
                days_history=40,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.70,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.60,
                duplicate_rate=0.0,
                schema_change_date=date(2026, 1, 10),
                output_dir=output_dir,
                raw_uri=f"file://{raw_dir}",
                labels_uri=f"file://{Path(tmp_dir) / 'labels'}",
                drift_start_date=date(2026, 1, 21),
                drift_amount_multiplier_end=2.0,
            )

            summary = generate_offline_transactions(config)

            self.assertTrue(summary["drift"]["enabled"])
            self.assertEqual(summary["drift"]["drift_start_date"], "2026-01-21")
            before = summary["drift"]["mean_amount_before_drift"]
            after = summary["drift"]["mean_amount_after_drift"]
            self.assertIsNotNone(before)
            self.assertIsNotNone(after)
            self.assertGreater(after, before * 1.3)
            self.assertLess(after, before * 1.9)

    def test_config_validate_rejects_drift_start_date_outside_history(self):
        """drift_start_date before start_date or at/after history end is rejected."""

        with TemporaryDirectory() as tmp_dir:
            base_kwargs = dict(
                random_seed=1,
                n_transactions=10,
                n_customers=10,
                n_accounts=10,
                n_merchants=5,
                start_date=date(2026, 1, 1),
                days_history=10,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.5,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.5,
                duplicate_rate=0.0,
                schema_change_date=date(2026, 1, 5),
                output_dir=Path(tmp_dir) / "out",
                raw_uri=f"file://{Path(tmp_dir) / 'raw'}",
            )

            too_early = OfflineGeneratorConfig(**base_kwargs, drift_start_date=date(2025, 12, 31))
            with self.assertRaises(ValueError):
                too_early.validate()

            too_late = OfflineGeneratorConfig(**base_kwargs, drift_start_date=date(2026, 1, 11))
            with self.assertRaises(ValueError):
                too_late.validate()

    def test_config_validate_rejects_non_positive_drift_multiplier(self):
        """drift_amount_multiplier_end must be greater than 0."""

        with TemporaryDirectory() as tmp_dir:
            config = OfflineGeneratorConfig(
                random_seed=1,
                n_transactions=10,
                n_customers=10,
                n_accounts=10,
                n_merchants=5,
                start_date=date(2026, 1, 1),
                days_history=10,
                currency="USD",
                skew_city="New York",
                skew_city_ratio=0.5,
                skew_merchant_category="online_marketplace",
                skew_merchant_category_ratio=0.5,
                duplicate_rate=0.0,
                schema_change_date=date(2026, 1, 5),
                output_dir=Path(tmp_dir) / "out",
                raw_uri=f"file://{Path(tmp_dir) / 'raw'}",
                drift_amount_multiplier_end=0.0,
            )
            with self.assertRaises(ValueError):
                config.validate()


if __name__ == "__main__":
    main()
