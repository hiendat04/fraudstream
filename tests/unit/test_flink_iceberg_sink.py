"""Unit tests for the Flink-to-Iceberg row mapping and DDL builders.

This module is skipped outside the isolated Python 3.12 PyFlink environment
(`uv run --project flink --python 3.12 python -m unittest ...`), the same
boundary `fraudstream.jobs.flink.iceberg_sink` itself keeps.
"""

from __future__ import annotations

import importlib.util
import json
from unittest import TestCase, main, skipUnless

from fraudstream.jobs.warehouse import IcebergCatalogConfig, PostgresJdbcConfig, WarehouseConfig


@skipUnless(importlib.util.find_spec("pyflink"), "PyFlink is not installed")
class FlinkIcebergSinkTest(TestCase):
    """Tests for the JSON-to-typed-row mapping and Iceberg DDL builders."""

    def test_clean_transaction_to_row_maps_every_field(self) -> None:
        """A clean-transaction JSON payload should map onto every declared row field."""

        from fraudstream.jobs.flink.iceberg_sink import CLEAN_TRANSACTIONS_ROW_TYPE, clean_transaction_to_row

        event = _sample_clean_event()
        row = clean_transaction_to_row(json.dumps(event))

        self.assertEqual(row["event_id"], "evt_001")
        self.assertEqual(row["amount_cents"], 1050)
        self.assertEqual(row["evaluation_is_fraud"], False)
        self.assertEqual(row["problem_flags"], [])
        self.assertIsNone(row["city"])
        self.assertEqual(set(CLEAN_TRANSACTIONS_ROW_TYPE.get_field_names()), set(event.keys()))

    def test_customer_feature_to_row_maps_shared_and_customer_fields(self) -> None:
        """A customer feature payload should map both the shared and entity-specific fields."""

        from fraudstream.jobs.flink.iceberg_sink import customer_feature_to_row

        feature = _sample_customer_feature()
        row = customer_feature_to_row(json.dumps(feature))

        self.assertEqual(row["feature_id"], feature["feature_id"])
        self.assertEqual(row["customer_id"], "cust_001")
        self.assertEqual(row["distinct_merchant_count"], 2)
        self.assertEqual(row["distinct_device_count"], 1)

    def test_merchant_feature_to_row_maps_shared_and_merchant_fields(self) -> None:
        """A merchant feature payload should map both the shared and entity-specific fields."""

        from fraudstream.jobs.flink.iceberg_sink import merchant_feature_to_row

        feature = _sample_merchant_feature()
        row = merchant_feature_to_row(json.dumps(feature))

        self.assertEqual(row["feature_id"], feature["feature_id"])
        self.assertEqual(row["merchant_id"], "merch_001")
        self.assertEqual(row["merchant_category"], "grocery")
        self.assertEqual(row["distinct_customer_count"], 4)

    def test_catalog_ddl_uses_jdbc_catalog_type_by_default(self) -> None:
        """The default `IcebergCatalogConfig` should produce a JDBC catalog statement."""

        from fraudstream.jobs.flink.iceberg_sink import _catalog_ddl

        ddl = _catalog_ddl(IcebergCatalogConfig(), PostgresJdbcConfig(), WarehouseConfig())
        self.assertIn("'catalog-impl' = 'org.apache.iceberg.jdbc.JdbcCatalog'", ddl)
        self.assertIn("'uri' = 'jdbc:postgresql://", ddl)

    def test_catalog_ddl_uses_hadoop_catalog_type_when_configured(self) -> None:
        """A `catalog_type='hadoop'` config should omit JDBC-only properties."""

        from fraudstream.jobs.flink.iceberg_sink import _catalog_ddl

        iceberg = IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri="file:///tmp/iceberg_warehouse")
        ddl = _catalog_ddl(iceberg, PostgresJdbcConfig(), WarehouseConfig())
        self.assertIn("'catalog-type' = 'hadoop'", ddl)
        self.assertNotIn("jdbc.user", ddl)

    def test_table_ddls_declare_primary_keys_for_upsert(self) -> None:
        """Every streaming table's DDL should declare the primary key upsert semantics rely on."""

        from fraudstream.jobs.flink.iceberg_sink import (
            _clean_transactions_ddl,
            _customer_features_ddl,
            _merchant_features_ddl,
        )

        iceberg = IcebergCatalogConfig()
        self.assertIn("PRIMARY KEY (event_id) NOT ENFORCED", _clean_transactions_ddl(iceberg))
        self.assertIn("PRIMARY KEY (feature_id) NOT ENFORCED", _customer_features_ddl(iceberg))
        self.assertIn("PRIMARY KEY (feature_id) NOT ENFORCED", _merchant_features_ddl(iceberg))
        for ddl in (
            _clean_transactions_ddl(iceberg),
            _customer_features_ddl(iceberg),
            _merchant_features_ddl(iceberg),
        ):
            self.assertIn("'write.upsert.enabled' = 'true'", ddl)


def _sample_clean_event() -> dict:
    """Return a minimal normalized clean-transaction event dict."""

    return {
        "event_id": "evt_001",
        "transaction_id": "txn_001",
        "account_id": "acct_001",
        "customer_id": "cust_001",
        "merchant_id": "merch_001",
        "merchant_category": "grocery",
        "amount": "10.50",
        "amount_cents": 1050,
        "currency": "USD",
        "city": None,
        "channel": "online",
        "transaction_status": "approved",
        "evaluation_is_fraud": False,
        "event_timestamp": "2026-01-01T00:00:00Z",
        "event_timestamp_ms": 1,
        "produced_at": "2026-01-01T00:00:01Z",
        "arrival_delay_seconds": 1,
        "device_id": "dev_001",
        "ip_address": "1.2.3.4",
        "authentication_method": None,
        "schema_version": "stream_v1",
        "problem_flags": [],
        "source_topic": "financial_transactions",
        "source_partition": 0,
        "source_sequence": 1,
        "partition_key": "cust_001",
    }


def _sample_customer_feature() -> dict:
    """Return a minimal customer feature record dict."""

    return {
        "feature_id": "customer_velocity_5m:cust_001:0",
        "feature_type": "customer_velocity_5m",
        "entity_type": "customer",
        "entity_id": "cust_001",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "window_size_minutes": 5,
        "txn_count": 3,
        "amount_sum": "30.00",
        "amount_avg": "10.00",
        "amount_max": "15.00",
        "declined_txn_count": 0,
        "last_event_timestamp": "2026-01-01T00:04:00Z",
        "is_correction": False,
        "emitted_at": "2026-01-01T00:05:01Z",
        "customer_id": "cust_001",
        "distinct_merchant_count": 2,
        "distinct_device_count": 1,
    }


def _sample_merchant_feature() -> dict:
    """Return a minimal merchant feature record dict."""

    return {
        "feature_id": "merchant_activity_5m:merch_001:0",
        "feature_type": "merchant_activity_5m",
        "entity_type": "merchant",
        "entity_id": "merch_001",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "window_size_minutes": 5,
        "txn_count": 8,
        "amount_sum": "80.00",
        "amount_avg": "10.00",
        "amount_max": "20.00",
        "declined_txn_count": 1,
        "last_event_timestamp": "2026-01-01T00:04:00Z",
        "is_correction": False,
        "emitted_at": "2026-01-01T00:05:01Z",
        "merchant_id": "merch_001",
        "merchant_category": "grocery",
        "distinct_customer_count": 4,
    }


if __name__ == "__main__":
    main()
