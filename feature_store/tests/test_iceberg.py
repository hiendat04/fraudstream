"""Spark/Iceberg wiring and landing tables the Feast offline store depends on."""

from __future__ import annotations

import unittest
import unittest.mock
from pathlib import Path
from tempfile import TemporaryDirectory

from fraudstream.jobs.warehouse import IcebergCatalogConfig, PostgresJdbcConfig, WarehouseConfig
from fraudstream_feast.spark import (
    CUSTOMER_PUSH_TABLE_COLUMNS,
    MERCHANT_PUSH_TABLE_COLUMNS,
    build_spark_session,
    configs_from_env,
    create_push_target_tables,
    feast_spark_conf,
)


class FeastSparkConfTest(unittest.TestCase):
    def test_conf_registers_the_iceberg_catalog_and_minio(self):
        """The generated spark_conf carries both the Iceberg catalog and the S3A settings."""

        conf = feast_spark_conf(
            warehouse=WarehouseConfig(
                uri="s3a://fraudstream/warehouse",
                endpoint="http://minio:9000",
                access_key="key",
                secret_key="secret",
            ),
            iceberg=IcebergCatalogConfig(
                catalog_name="iceberg",
                catalog_type="jdbc",
                warehouse_uri="s3a://fraudstream/warehouse",
            ),
            postgres=PostgresJdbcConfig(host="postgres", database="fraudstream"),
        )

        self.assertEqual(
            conf["spark.sql.catalog.iceberg.catalog-impl"], "org.apache.iceberg.jdbc.JdbcCatalog"
        )
        self.assertNotIn("spark.sql.catalog.iceberg.type", conf)
        self.assertEqual(conf["spark.sql.catalog.iceberg.warehouse"], "s3a://fraudstream/warehouse")
        self.assertEqual(conf["spark.hadoop.fs.s3a.endpoint"], "http://minio:9000")
        self.assertIn("iceberg", conf["spark.jars.packages"])

    def test_conf_uses_type_hadoop_with_no_catalog_impl_for_the_hadoop_catalog(self):
        """Iceberg's CatalogUtil raises when both `type` and `catalog-impl` are set on one catalog."""

        conf = feast_spark_conf(
            warehouse=WarehouseConfig(),
            iceberg=IcebergCatalogConfig(catalog_type="hadoop"),
            postgres=PostgresJdbcConfig(),
        )

        self.assertEqual(conf["spark.sql.catalog.iceberg.type"], "hadoop")
        self.assertNotIn("spark.sql.catalog.iceberg.catalog-impl", conf)

    def test_conf_rejects_an_unsupported_catalog_type(self):
        """An env var typo must raise here, since the env path has no argparse `choices` to catch it."""

        with self.assertRaises(ValueError):
            feast_spark_conf(
                warehouse=WarehouseConfig(),
                iceberg=IcebergCatalogConfig(catalog_type="not-a-real-type"),
                postgres=PostgresJdbcConfig(),
            )

    def test_conf_values_are_all_strings(self):
        """feature_store.yaml is YAML, so every spark_conf value must serialize as a string."""

        conf = feast_spark_conf(
            warehouse=WarehouseConfig(),
            iceberg=IcebergCatalogConfig(),
            postgres=PostgresJdbcConfig(),
        )

        for key, value in conf.items():
            self.assertIsInstance(value, str, msg=f"{key} is not a string")


class ConfigsFromEnvTest(unittest.TestCase):
    """`PostgresJdbcConfig()` defaults to localhost -- only argparse reads the env.

    Both entry points in this project get their arguments from the container
    environment rather than `sys.argv`, so they need an env-reading constructor
    or they silently try to reach PostgreSQL on localhost from inside a
    container where it lives on the `postgres` host.
    """

    def test_all_three_configs_come_from_the_container_environment(self):
        env = {
            "FRAUDSTREAM_WAREHOUSE_URI": "s3a://fraudstream/warehouse",
            "MINIO_ENDPOINT": "http://minio:9000",
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "fraudstream",
            "POSTGRES_USER": "fraudstream",
            "POSTGRES_PASSWORD": "pg-secret",
        }

        with unittest.mock.patch.dict("os.environ", env, clear=False):
            warehouse, iceberg, postgres = configs_from_env()

        self.assertEqual(warehouse.endpoint, "http://minio:9000")
        self.assertEqual(postgres.url, "jdbc:postgresql://postgres:5432/fraudstream")
        self.assertEqual(iceberg.warehouse_uri, "s3a://fraudstream/warehouse")


class PushTargetTableTest(unittest.TestCase):
    """Landing tables plus the exact read/append sequence Feast performs on them.

    Feast addresses an Iceberg push target by `path` (a catalog-qualified table
    name) plus `file_format="iceberg"`, then does
    `read.format("iceberg").load(path)` followed by
    `write.format("iceberg").mode("append").save(path)`. If either fails
    against a real Iceberg table, offline push cannot work at all -- so it is
    asserted here, before any Feast object exists.
    """

    @classmethod
    def setUpClass(cls):
        cls._tmp_dir = TemporaryDirectory()
        warehouse_uri = Path(cls._tmp_dir.name).joinpath("warehouse").as_uri()
        cls.spark = build_spark_session(
            warehouse=WarehouseConfig(uri=warehouse_uri),
            iceberg=IcebergCatalogConfig(catalog_type="hadoop", warehouse_uri=warehouse_uri),
            postgres=PostgresJdbcConfig(),
            app_name="PushTargetTableTest",
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls._tmp_dir.cleanup()

    def test_tables_are_created_with_the_declared_column_order(self):
        """Feast matches a pushed dataframe against the table's columns, so names are a contract."""

        customer_table, merchant_table = create_push_target_tables(self.spark, catalog_name="iceberg")

        self.assertEqual(tuple(self.spark.table(customer_table).columns), CUSTOMER_PUSH_TABLE_COLUMNS)
        self.assertEqual(tuple(self.spark.table(merchant_table).columns), MERCHANT_PUSH_TABLE_COLUMNS)

    def test_creation_is_idempotent(self):
        """The push job calls this on every start, so a second run must be a no-op."""

        first = create_push_target_tables(self.spark, catalog_name="iceberg")
        second = create_push_target_tables(self.spark, catalog_name="iceberg")

        self.assertEqual(first, second)

    def test_append_through_format_iceberg_and_path(self):
        """This is exactly what Feast's offline_write_batch does; if it fails, push cannot work."""

        customer_table, _ = create_push_target_tables(self.spark, catalog_name="iceberg")
        before = self.spark.read.format("iceberg").load(customer_table).count()

        row = {column: None for column in CUSTOMER_PUSH_TABLE_COLUMNS}
        row["customer_id"] = "C-1"
        batch = self.spark.createDataFrame(
            [tuple(row[column] for column in CUSTOMER_PUSH_TABLE_COLUMNS)],
            schema=self.spark.table(customer_table).schema,
        )
        batch.write.format("iceberg").mode("append").save(customer_table)

        after = self.spark.read.format("iceberg").load(customer_table)
        self.assertEqual(after.count(), before + 1)
        self.assertIn("C-1", [r["customer_id"] for r in after.collect()])


if __name__ == "__main__":
    unittest.main()
