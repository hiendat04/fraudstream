"""Unit tests for the offline transaction generator."""

from __future__ import annotations

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
            self.assertTrue(any(config.output_dir.glob("schema_version=v1/transaction_date=*/transactions.csv")))
            self.assertTrue(any(config.output_dir.glob("schema_version=v2/transaction_date=*/transactions.csv")))


if __name__ == "__main__":
    main()
