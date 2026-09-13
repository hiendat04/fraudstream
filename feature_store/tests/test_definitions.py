"""Contract tests for the Feast feature definitions."""

from __future__ import annotations

import os
import unittest
import unittest.mock
from datetime import timedelta
from pathlib import Path

import yaml
from feast import FeatureView, PushSource

from feature_repo import definitions
from fraudstream_feast.spark import configs_from_env, feast_spark_conf


ALL_FEATURE_VIEWS = (
    "customer_rolling_features",
    "customer_orders_90d_features",
    "merchant_risk_features",
    "customer_features_5m_stream",
    "merchant_features_5m_stream",
)

_FEATURE_STORE_YAML = Path(__file__).resolve().parents[1] / "feature_repo" / "feature_store.yaml"

_ENV_FOR_YAML_DIFF = {
    "POSTGRES_USER": "fraudstream",
    "POSTGRES_PASSWORD": "pg-secret",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fraudstream",
    "REDIS_HOST": "redis",
    "REDIS_PORT": "6379",
    "FEAST_SPARK_MASTER": "local[4]",
    "FRAUDSTREAM_WAREHOUSE_URI": "s3a://fraudstream/warehouse",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "key",
    "MINIO_SECRET_KEY": "secret",
    # feature_store.yaml always encodes the jdbc catalog branch under the
    # "iceberg" name, so configs_from_env() must be pinned to the same
    # values here rather than inheriting whatever FRAUDSTREAM_ICEBERG_* the
    # ambient shell happens to have set -- otherwise a locally-exported
    # catalog_type="hadoop" would build the wrong conf branch and fail this
    # test for a reason unrelated to real drift.
    "FRAUDSTREAM_ICEBERG_CATALOG_NAME": "iceberg",
    "FRAUDSTREAM_ICEBERG_CATALOG_TYPE": "jdbc",
    "FRAUDSTREAM_ICEBERG_WAREHOUSE_URI": "s3a://fraudstream/warehouse",
}


class DefinitionsTest(unittest.TestCase):
    def _views(self) -> dict[str, FeatureView]:
        """Return the five feature views keyed by name."""

        return {name: getattr(definitions, name) for name in ALL_FEATURE_VIEWS}

    def test_every_expected_feature_view_exists(self):
        """The repo defines exactly the three batch and two streaming views."""

        for name, view in self._views().items():
            self.assertIsInstance(view, FeatureView, msg=name)
            self.assertEqual(view.name, name)

    def test_no_feature_view_exposes_the_fraud_label(self):
        """Labels are joined from gold.transaction_labels at training time, never served as a feature."""

        for name, view in self._views().items():
            field_names = {field.name for field in view.schema}
            self.assertNotIn("is_fraud", field_names, msg=name)
            self.assertNotIn("evaluation_is_fraud", field_names, msg=name)

    def test_no_source_reads_the_training_table(self):
        """feat_transaction_training carries is_fraud, so no source may reference it."""

        for name, view in self._views().items():
            source_text = " ".join(
                str(getattr(view.batch_source, attr, "") or "")
                for attr in ("query", "table", "path")
            )
            self.assertNotIn("feat_transaction_training", source_text, msg=name)

    def test_entities_use_natural_join_keys(self):
        """Batch and streaming views must agree on join keys or cannot be requested together."""

        self.assertEqual(definitions.customer.join_key, "customer_id")
        self.assertEqual(definitions.merchant.join_key, "merchant_id")
        for name in ("customer_rolling_features", "customer_orders_90d_features", "customer_features_5m_stream"):
            self.assertEqual(getattr(definitions, name).entities, ["customer"], msg=name)
        for name in ("merchant_risk_features", "merchant_features_5m_stream"):
            self.assertEqual(getattr(definitions, name).entities, ["merchant"], msg=name)

    def test_ttls_match_the_documented_values(self):
        """TTLs are a documented contract; changing one must break this test."""

        self.assertEqual(definitions.customer_rolling_features.ttl, timedelta(days=31))
        self.assertEqual(definitions.customer_orders_90d_features.ttl, timedelta(days=91))
        self.assertEqual(definitions.merchant_risk_features.ttl, timedelta(days=31))
        self.assertEqual(definitions.customer_features_5m_stream.ttl, timedelta(hours=1))
        self.assertEqual(definitions.merchant_features_5m_stream.ttl, timedelta(hours=1))

    def test_batch_sources_shift_the_snapshot_timestamp_forward_one_day(self):
        """A daily snapshot is only knowable after its day ends, so its timestamp is feature_date + 1."""

        for name in ("customer_rolling_features", "customer_orders_90d_features", "merchant_risk_features"):
            query = getattr(definitions, name).batch_source.query
            self.assertIn("INTERVAL 1 DAY", query, msg=name)

    def test_stream_views_are_backed_by_appendable_iceberg_push_sources(self):
        """Feast's Spark offline store cannot write Iceberg, so these sources are read-only `table=` references to the Task 1 landing tables; the stream-push job writes them directly with Spark."""

        expected_tables = {
            "customer_features_5m_stream": "iceberg.feature_store.stream_customer_features_5m",
            "merchant_features_5m_stream": "iceberg.feature_store.stream_merchant_features_5m",
        }
        for name, expected_table in expected_tables.items():
            view = getattr(definitions, name)
            self.assertIsInstance(view.stream_source, PushSource)
            batch_source = view.stream_source.batch_source
            self.assertEqual(batch_source.table, expected_table, msg=name)
            self.assertIsNone(batch_source.query, msg=name)
            self.assertIsNone(batch_source.path, msg=name)

    def test_stream_view_schemas_match_the_landing_tables(self):
        """A pushed dataframe must carry exactly the batch source's columns, so the two must agree."""

        from fraudstream_feast.spark import CUSTOMER_PUSH_TABLE_COLUMNS, MERCHANT_PUSH_TABLE_COLUMNS

        customer_fields = {f.name for f in definitions.customer_features_5m_stream.schema}
        merchant_fields = {f.name for f in definitions.merchant_features_5m_stream.schema}
        timestamps = {"event_timestamp", "created"}

        self.assertEqual(customer_fields, set(CUSTOMER_PUSH_TABLE_COLUMNS) - timestamps - {"customer_id"})
        self.assertEqual(merchant_fields, set(MERCHANT_PUSH_TABLE_COLUMNS) - timestamps - {"merchant_id"})


class FeatureStoreYamlMatchesFeastSparkConfTest(unittest.TestCase):
    """`feature_store.yaml` hand-copies `feast_spark_conf`'s settings; this checks they still agree.

    Feast cannot load a Python dict as its offline-store config -- it only
    reads `feature_store.yaml` -- so the Spark settings have to be written out
    twice. Expanding both sides under the same environment and comparing them
    key-by-key catches drift a human eyeballing two files would miss.
    """

    def test_yaml_spark_conf_matches_feast_spark_conf(self):
        """Every key `feast_spark_conf` produces must appear in the YAML with the same value."""

        with unittest.mock.patch.dict(os.environ, _ENV_FOR_YAML_DIFF, clear=False):
            raw_yaml = os.path.expandvars(_FEATURE_STORE_YAML.read_text())
            yaml_spark_conf = yaml.safe_load(raw_yaml)["offline_store"]["spark_conf"]

            warehouse, iceberg, postgres = configs_from_env()
            expected_conf = feast_spark_conf(warehouse=warehouse, iceberg=iceberg, postgres=postgres)

        for key, value in expected_conf.items():
            self.assertIn(key, yaml_spark_conf)
            self.assertEqual(str(yaml_spark_conf[key]), value, msg=key)

        extra_keys = set(yaml_spark_conf) - set(expected_conf)
        self.assertEqual(extra_keys, {"spark.master", "spark.app.name"})


if __name__ == "__main__":
    unittest.main()
