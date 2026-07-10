"""Unit tests for the PostgreSQL Parquet publisher."""

from __future__ import annotations

from datetime import UTC, date, datetime
from math import nan
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from fraudstream.jobs.postgres.publish import (
    GOLD_TABLE_COLUMNS,
    PostgresPublishConfig,
    TablePublishSpec,
    _build_insert_sql,
    _normalize_postgres_value,
    _resolve_existing_specs,
    _select_table_specs,
    publish_parquet_to_postgres,
)


class PostgresPublishTest(TestCase):
    """Tests for PostgreSQL publish configuration and helpers."""

    def test_selects_silver_table_specs(self) -> None:
        specs = _select_table_specs(PostgresPublishConfig(layer="silver"))

        self.assertEqual(
            [spec.target_table for spec in specs],
            ["silver.stg_transactions", "silver.stg_transaction_quality_issues"],
        )
        self.assertIn("quality_issue_codes", specs[0].columns)
        self.assertIn("_silver_record_action", specs[1].columns)

    def test_filters_gold_table_specs_by_table_name(self) -> None:
        specs = _select_table_specs(
            PostgresPublishConfig(
                layer="gold",
                selected_tables=("dim_customer", "gold.fact_transactions"),
            )
        )

        self.assertEqual(
            [spec.target_table for spec in specs],
            ["gold.dim_customer", "gold.fact_transactions"],
        )

    def test_builds_quoted_insert_sql(self) -> None:
        insert_sql = _build_insert_sql(
            "silver.stg_transactions",
            ("transaction_id", "_silver_processed_at"),
        )

        self.assertEqual(
            insert_sql,
            (
                'INSERT INTO "silver"."stg_transactions" '
                '("transaction_id", "_silver_processed_at") VALUES (%s, %s)'
            ),
        )

    def test_normalizes_values_for_psycopg(self) -> None:
        naive_datetime = datetime(2026, 7, 8, 12, 30, 0)

        self.assertEqual(_normalize_postgres_value(date(2026, 7, 8)), date(2026, 7, 8))
        self.assertEqual(_normalize_postgres_value(nan), None)
        self.assertEqual(_normalize_postgres_value(naive_datetime).tzinfo, UTC)
        self.assertEqual(_normalize_postgres_value(["a", nan, None]), ["a", None, None])

    def test_skips_missing_sources_when_enabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "missing"
            specs = (
                TablePublishSpec(
                    name="missing_table",
                    source_path=missing_path,
                    target_table="gold.dim_customer",
                    columns=("customer_id",),
                ),
            )

            existing_specs, skipped_results = _resolve_existing_specs(specs, skip_missing=True)

        self.assertEqual(existing_specs, ())
        self.assertEqual(len(skipped_results), 1)
        self.assertEqual(skipped_results[0].status, "skipped")
        self.assertEqual(skipped_results[0].target_table, "gold.dim_customer")

    def test_publish_returns_skipped_result_when_all_selected_sources_are_missing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            result = publish_parquet_to_postgres(
                PostgresPublishConfig(
                    layer="gold",
                    gold_dir=Path(tmp_dir) / "gold",
                    selected_tables=("dim_customer",),
                    skip_missing=True,
                )
            )

        self.assertEqual(result.published_row_count, 0)
        self.assertEqual(len(result.table_results), 1)
        self.assertEqual(result.table_results[0].status, "skipped")
        self.assertEqual(result.table_results[0].target_table, "gold.dim_customer")

    def test_gold_specs_include_scd2_and_feature_time_columns(self) -> None:
        for table_name in ("dim_customer", "dim_account", "dim_merchant"):
            self.assertIn("valid_from_ts", GOLD_TABLE_COLUMNS[table_name])
            self.assertIn("valid_to_ts", GOLD_TABLE_COLUMNS[table_name])
            self.assertIn("is_current", GOLD_TABLE_COLUMNS[table_name])

        for table_name in (
            "feat_customer_rolling",
            "feat_customer_total_orders_90d",
            "feat_transaction_training",
        ):
            self.assertIn("event_timestamp", GOLD_TABLE_COLUMNS[table_name])
            self.assertIn("created", GOLD_TABLE_COLUMNS[table_name])


if __name__ == "__main__":
    main()
