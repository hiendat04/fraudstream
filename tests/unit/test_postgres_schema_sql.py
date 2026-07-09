"""Validate the PostgreSQL schema contract used for local serving."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


SCHEMA_SQL_PATH = Path("infra/postgres/init/001_create_fraudstream_schema.sql")
DOCKER_COMPOSE_PATH = Path("docker-compose.yml")


class PostgresSchemaSqlTest(unittest.TestCase):
    """Lightweight checks for the DBeaver/DataHub serving schema."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema_sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        cls.docker_compose = DOCKER_COMPOSE_PATH.read_text(encoding="utf-8")

    def test_creates_all_zone_schemas(self) -> None:
        for schema_name in ("metadata", "bronze", "silver", "gold"):
            self.assertIn(f"CREATE SCHEMA IF NOT EXISTS {schema_name};", self.schema_sql)

    def test_includes_bronze_and_silver_tables(self) -> None:
        expected_tables = [
            "bronze.raw_transaction_ingest_runs",
            "bronze.raw_transactions",
            "silver.stg_transactions",
            "silver.stg_transaction_quality_issues",
        ]

        for table_name in expected_tables:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table_name}", self.schema_sql)

    def test_includes_gold_snowflake_tables(self) -> None:
        expected_tables = [
            "gold.fact_transactions",
            "gold.fact_transaction_quality_issue",
            "gold.dim_customer",
            "gold.dim_account",
            "gold.dim_merchant",
            "gold.dim_merchant_category",
            "gold.dim_city",
            "gold.dim_date",
            "gold.dim_channel",
            "gold.dim_quality_issue",
            "gold.fact_customer_daily",
            "gold.fact_account_daily",
            "gold.fact_merchant_daily",
            "gold.fact_city_category_daily",
            "gold.fact_device_ip_daily",
            "gold.feat_customer_rolling",
            "gold.feat_customer_total_orders_90d",
            "gold.feat_transaction_training",
        ]

        for table_name in expected_tables:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table_name}", self.schema_sql)

        self.assertIn("CREATE OR REPLACE VIEW gold.obt_transaction_enriched", self.schema_sql)

    def test_scd2_dimensions_have_required_columns(self) -> None:
        for table_name in ("dim_customer", "dim_account", "dim_merchant"):
            table_body = self._table_body(f"gold.{table_name}")
            self.assertIn("valid_from_ts TIMESTAMPTZ NOT NULL", table_body)
            self.assertIn("valid_to_ts TIMESTAMPTZ", table_body)
            self.assertIn("is_current BOOLEAN NOT NULL DEFAULT true", table_body)

    def test_feature_tables_have_event_timestamp_and_created(self) -> None:
        for table_name in (
            "gold.feat_customer_rolling",
            "gold.feat_customer_total_orders_90d",
            "gold.feat_transaction_training",
        ):
            table_body = self._table_body(table_name)
            self.assertIn("event_timestamp TIMESTAMPTZ NOT NULL", table_body)
            self.assertIn("created TIMESTAMPTZ NOT NULL DEFAULT now()", table_body)

    def test_docker_compose_runs_postgres_schema_init(self) -> None:
        self.assertIn("postgres:", self.docker_compose)
        self.assertIn("postgres-schema-init:", self.docker_compose)
        self.assertIn("POSTGRES_DB: \"${POSTGRES_DB:-fraudstream}\"", self.docker_compose)
        self.assertIn("001_create_fraudstream_schema.sql", self.docker_compose)

    def _table_body(self, table_name: str) -> str:
        pattern = rf"CREATE TABLE IF NOT EXISTS {re.escape(table_name)} \((.*?)\);"
        match = re.search(pattern, self.schema_sql, flags=re.DOTALL)
        self.assertIsNotNone(match, f"missing table definition for {table_name}")
        return match.group(1)


if __name__ == "__main__":
    unittest.main()
