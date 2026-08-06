"""Unit tests for the shared MinIO/Iceberg/PostgreSQL warehouse helpers."""

from __future__ import annotations

from unittest import TestCase, main

from fraudstream.jobs.warehouse import (
    HADOOP_AWS_PACKAGE,
    ICEBERG_SPARK_PACKAGE,
    POSTGRESQL_JDBC_PACKAGE,
    IcebergCatalogConfig,
    PostgresJdbcConfig,
    WarehouseConfig,
    configure_iceberg_catalog,
    configure_object_storage,
    write_iceberg_table,
)


class _RecordingBuilder:
    """A minimal stand-in for `SparkSession.builder` that records `.config(...)` calls."""

    def __init__(self) -> None:
        self.options: dict[str, str] = {}

    def config(self, key: str, value: str) -> "_RecordingBuilder":
        self.options[key] = value
        return self


class ConfigureIcebergCatalogTest(TestCase):
    """Tests for `configure_iceberg_catalog`'s JDBC and Hadoop catalog branches."""

    def test_jdbc_catalog_type_sets_postgres_connection_properties(self) -> None:
        """The default JDBC catalog should point at the given PostgreSQL connection."""

        builder = configure_iceberg_catalog(
            _RecordingBuilder(),
            IcebergCatalogConfig(catalog_name="iceberg"),
            PostgresJdbcConfig(host="pg-host", port=5432, database="fraudstream", user="u", password="p"),
        )

        self.assertEqual(
            builder.options["spark.sql.catalog.iceberg.catalog-impl"],
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        self.assertEqual(builder.options["spark.sql.catalog.iceberg.uri"], "jdbc:postgresql://pg-host:5432/fraudstream")
        self.assertEqual(builder.options["spark.sql.catalog.iceberg.jdbc.user"], "u")
        self.assertEqual(builder.options["spark.sql.catalog.iceberg.jdbc.password"], "p")
        self.assertEqual(
            builder.options["spark.sql.catalog.iceberg.io-impl"],
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        self.assertIn(ICEBERG_SPARK_PACKAGE, builder.options["spark.jars.packages"])
        self.assertIn(HADOOP_AWS_PACKAGE, builder.options["spark.jars.packages"])
        self.assertIn(POSTGRESQL_JDBC_PACKAGE, builder.options["spark.jars.packages"])

    def test_hadoop_catalog_type_omits_jdbc_properties(self) -> None:
        """The Hadoop catalog (unit tests only) should not reference PostgreSQL at all."""

        builder = configure_iceberg_catalog(
            _RecordingBuilder(),
            IcebergCatalogConfig(catalog_name="iceberg", catalog_type="hadoop", warehouse_uri="file:///tmp/warehouse"),
            PostgresJdbcConfig(),
        )

        self.assertEqual(builder.options["spark.sql.catalog.iceberg.type"], "hadoop")
        self.assertEqual(builder.options["spark.sql.catalog.iceberg.warehouse"], "file:///tmp/warehouse")
        self.assertNotIn("spark.sql.catalog.iceberg.catalog-impl", builder.options)
        self.assertNotIn("spark.sql.catalog.iceberg.uri", builder.options)

    def test_custom_catalog_name_changes_config_prefix(self) -> None:
        """A non-default catalog name should be reflected in every Spark config key."""

        builder = configure_iceberg_catalog(
            _RecordingBuilder(),
            IcebergCatalogConfig(catalog_name="lakehouse"),
            PostgresJdbcConfig(),
        )

        self.assertIn("spark.sql.catalog.lakehouse", builder.options)
        self.assertNotIn("spark.sql.catalog.iceberg", builder.options)

    def test_invalid_catalog_type_is_rejected(self) -> None:
        """An unsupported catalog_type should raise before touching the builder."""

        with self.assertRaises(ValueError):
            configure_iceberg_catalog(
                _RecordingBuilder(),
                IcebergCatalogConfig(catalog_type="rest"),
                PostgresJdbcConfig(),
            )


class ConfigureObjectStorageTest(TestCase):
    """Tests for the existing MinIO/S3A helper, to guard against accidental regressions."""

    def test_sets_s3a_endpoint_and_credentials(self) -> None:
        builder = configure_object_storage(
            _RecordingBuilder(),
            WarehouseConfig(endpoint="http://minio:9000", access_key="ak", secret_key="sk"),
        )

        self.assertEqual(builder.options["spark.hadoop.fs.s3a.endpoint"], "http://minio:9000")
        self.assertEqual(builder.options["spark.hadoop.fs.s3a.access.key"], "ak")
        self.assertEqual(builder.options["spark.hadoop.fs.s3a.secret.key"], "sk")


class WriteIcebergTableTest(TestCase):
    """Tests for `write_iceberg_table`'s mode branching, using a fake DataFrame writer chain."""

    def test_overwrite_mode_calls_create_or_replace(self) -> None:
        dataframe = _FakeDataFrame()
        write_iceberg_table(dataframe, "iceberg.bronze.raw_transactions", ["event_date"], "overwrite")

        writer = dataframe.write_to_calls[0]
        self.assertEqual(writer.table, "iceberg.bronze.raw_transactions")
        self.assertEqual(writer.partition_columns, ("event_date",))
        self.assertEqual(writer.calls, ["using:iceberg", "partitionedBy:('event_date',)", "createOrReplace"])

    def test_append_mode_creates_missing_table_then_appends(self) -> None:
        dataframe = _FakeDataFrame(table_exists=False)
        write_iceberg_table(dataframe, "iceberg.bronze.raw_transaction_ingest_runs", [], "append")

        writer = dataframe.write_to_calls[0]
        self.assertEqual(writer.calls, ["using:iceberg", "create"])

    def test_append_mode_appends_to_existing_table(self) -> None:
        dataframe = _FakeDataFrame(table_exists=True)
        write_iceberg_table(dataframe, "iceberg.bronze.raw_transaction_ingest_runs", [], "append")

        writer = dataframe.write_to_calls[0]
        self.assertEqual(writer.calls, ["using:iceberg", "append"])

    def test_unsupported_mode_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            write_iceberg_table(_FakeDataFrame(), "iceberg.bronze.raw_transactions", [], "ignore")


class _FakeWriter:
    """Records the `writeTo(...)` chain `write_iceberg_table` builds."""

    def __init__(self, table: str) -> None:
        self.table = table
        self.partition_columns: tuple[str, ...] = ()
        self.calls: list[str] = []

    def using(self, provider: str) -> "_FakeWriter":
        self.calls.append(f"using:{provider}")
        return self

    def partitionedBy(self, *columns: str) -> "_FakeWriter":
        self.partition_columns = columns
        self.calls.append(f"partitionedBy:{columns}")
        return self

    def createOrReplace(self) -> None:
        self.calls.append("createOrReplace")

    def create(self) -> None:
        self.calls.append("create")

    def append(self) -> None:
        self.calls.append("append")


class _FakeCatalog:
    def __init__(self, table_exists: bool) -> None:
        self._table_exists = table_exists

    def tableExists(self, table: str) -> bool:
        del table
        return self._table_exists


class _FakeSparkSession:
    def __init__(self, table_exists: bool) -> None:
        self.catalog = _FakeCatalog(table_exists)


class _FakeDataFrame:
    """A minimal stand-in for a Spark DataFrame's `writeTo(...)` entry point."""

    def __init__(self, table_exists: bool = False) -> None:
        self.sparkSession = _FakeSparkSession(table_exists)
        self.write_to_calls: list[_FakeWriter] = []

    def writeTo(self, table: str) -> _FakeWriter:
        writer = _FakeWriter(table)
        self.write_to_calls.append(writer)
        return writer


if __name__ == "__main__":
    main()
