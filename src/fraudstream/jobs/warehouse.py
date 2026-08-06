"""Shared MinIO (S3A) object storage and direct-to-PostgreSQL JDBC helpers.

Every offline Spark job (Bronze, Silver, Gold, offline features) reads and
writes its own Parquet tables in MinIO and writes its own PostgreSQL tables
over JDBC at the end of its run. This module centralizes the Spark session
configuration and CLI conventions those jobs share
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any, Sequence


DEFAULT_WAREHOUSE_URI = "s3a://fraudstream/warehouse"
DEFAULT_MINIO_ENDPOINT = "http://localhost:19000"
DEFAULT_MINIO_ACCESS_KEY = "fraudstream"
DEFAULT_MINIO_SECRET_KEY = "fraudstream_local_password"

DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DATABASE = "fraudstream"
DEFAULT_POSTGRES_USER = "fraudstream"
DEFAULT_POSTGRES_PASSWORD = "fraudstream_local_password"

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"
POSTGRESQL_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"
ICEBERG_SPARK_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"

DEFAULT_ICEBERG_CATALOG_NAME = "iceberg"
DEFAULT_ICEBERG_CATALOG_TYPE = "jdbc"
SUPPORTED_ICEBERG_CATALOG_TYPES = {"jdbc", "hadoop"}


@dataclass(frozen=True)
class WarehouseConfig:
    """MinIO / S3A location and credentials for Parquet storage."""

    uri: str = DEFAULT_WAREHOUSE_URI
    endpoint: str = DEFAULT_MINIO_ENDPOINT
    access_key: str = DEFAULT_MINIO_ACCESS_KEY
    secret_key: str = DEFAULT_MINIO_SECRET_KEY

    @classmethod
    def from_env(cls) -> "WarehouseConfig":
        """Build a WarehouseConfig from the same env vars `add_warehouse_arguments` reads.

        For code that talks to MinIO outside of a job's own CLI parsing --
        e.g. an Airflow `PythonOperator` callable, which gets its arguments
        from `op_kwargs`, not `sys.argv`, but still runs inside a container
        where `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` are set.
        """

        return cls(
            uri=_env_value("FRAUDSTREAM_WAREHOUSE_URI", DEFAULT_WAREHOUSE_URI),
            endpoint=_env_value("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
            access_key=_env_value("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY),
            secret_key=_env_value("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY),
        )


@dataclass(frozen=True)
class IcebergCatalogConfig:
    """Iceberg catalog settings.

    `catalog_type="jdbc"` (the default, used by every real job) backs the
    catalog with the same PostgreSQL instance the jobs already write to over
    plain JDBC -- it only stores table metadata (which files make up which
    table version) there; table data still lives in MinIO under
    `warehouse_uri`, in Iceberg's own layout.

    `catalog_type="hadoop"` stores catalog metadata directly under
    `warehouse_uri` instead, with no database involved. Unit tests use this
    with a local `file://` `warehouse_uri` so they can exercise real Iceberg
    reads/writes without a running Postgres, the same way they already run
    Spark against a local filesystem path instead of real MinIO.
    """

    catalog_name: str = DEFAULT_ICEBERG_CATALOG_NAME
    catalog_type: str = DEFAULT_ICEBERG_CATALOG_TYPE
    warehouse_uri: str = DEFAULT_WAREHOUSE_URI

    def validate(self) -> None:
        """Raise when the Iceberg catalog config is not usable."""

        if self.catalog_type not in SUPPORTED_ICEBERG_CATALOG_TYPES:
            allowed = ", ".join(sorted(SUPPORTED_ICEBERG_CATALOG_TYPES))
            raise ValueError(f"catalog_type must be one of: {allowed}")


@dataclass(frozen=True)
class PostgresJdbcConfig:
    """PostgreSQL JDBC connection settings for direct Spark writes."""

    host: str = DEFAULT_POSTGRES_HOST
    port: int = DEFAULT_POSTGRES_PORT
    database: str = DEFAULT_POSTGRES_DATABASE
    user: str = DEFAULT_POSTGRES_USER
    password: str = DEFAULT_POSTGRES_PASSWORD

    @property
    def url(self) -> str:
        """Return the JDBC connection URL for this PostgreSQL database."""

        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


def warehouse_path(uri: str, *parts: str) -> str:
    """Join a warehouse root URI with one or more path segments.

    `uri` is a plain string (e.g. `s3a://fraudstream/warehouse`), not a
    `pathlib.Path` -- `Path` normalizes `//` and breaks URI schemes.
    """

    return "/".join((uri.rstrip("/"), *parts))


def add_warehouse_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the shared MinIO/S3A warehouse arguments to one job parser."""

    parser.add_argument(
        "--warehouse-uri",
        default=_env_value("FRAUDSTREAM_WAREHOUSE_URI", DEFAULT_WAREHOUSE_URI),
        help=f"S3A root URI for Parquet tables. Defaults to {DEFAULT_WAREHOUSE_URI}.",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=_env_value("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
        help=f"MinIO S3 API endpoint. Defaults to {DEFAULT_MINIO_ENDPOINT}.",
    )
    parser.add_argument(
        "--minio-access-key",
        default=_env_value("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY),
    )
    parser.add_argument(
        "--minio-secret-key",
        default=_env_value("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY),
    )


def warehouse_config_from_args(args: argparse.Namespace) -> WarehouseConfig:
    """Build a WarehouseConfig from parsed CLI arguments."""

    return WarehouseConfig(
        uri=args.warehouse_uri,
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
    )


def add_iceberg_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the shared Iceberg catalog arguments to one job parser."""

    parser.add_argument(
        "--iceberg-catalog-name",
        default=_env_value("FRAUDSTREAM_ICEBERG_CATALOG_NAME", DEFAULT_ICEBERG_CATALOG_NAME),
        help=f"Spark SQL catalog name registered for Iceberg. Defaults to {DEFAULT_ICEBERG_CATALOG_NAME}.",
    )
    parser.add_argument(
        "--iceberg-warehouse-uri",
        default=_env_value("FRAUDSTREAM_ICEBERG_WAREHOUSE_URI", DEFAULT_WAREHOUSE_URI),
        help=f"S3A root URI Iceberg manages its own table layout under. Defaults to {DEFAULT_WAREHOUSE_URI}.",
    )
    parser.add_argument(
        "--iceberg-catalog-type",
        choices=sorted(SUPPORTED_ICEBERG_CATALOG_TYPES),
        default=_env_value("FRAUDSTREAM_ICEBERG_CATALOG_TYPE", DEFAULT_ICEBERG_CATALOG_TYPE),
        help=f"Iceberg catalog backend. Defaults to {DEFAULT_ICEBERG_CATALOG_TYPE} (PostgreSQL-backed).",
    )


def iceberg_config_from_args(args: argparse.Namespace) -> IcebergCatalogConfig:
    """Build an IcebergCatalogConfig from parsed CLI arguments."""

    return IcebergCatalogConfig(
        catalog_name=args.iceberg_catalog_name,
        catalog_type=args.iceberg_catalog_type,
        warehouse_uri=args.iceberg_warehouse_uri,
    )


def add_postgres_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the shared PostgreSQL JDBC arguments to one job parser."""

    parser.add_argument("--postgres-host", default=_env_value("POSTGRES_HOST", DEFAULT_POSTGRES_HOST))
    parser.add_argument("--postgres-port", type=int, default=_env_int("POSTGRES_PORT", DEFAULT_POSTGRES_PORT))
    parser.add_argument("--postgres-db", default=_env_value("POSTGRES_DB", DEFAULT_POSTGRES_DATABASE))
    parser.add_argument("--postgres-user", default=_env_value("POSTGRES_USER", DEFAULT_POSTGRES_USER))
    parser.add_argument("--postgres-password", default=_env_value("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD))


def postgres_config_from_args(args: argparse.Namespace) -> PostgresJdbcConfig:
    """Build a PostgresJdbcConfig from parsed CLI arguments."""

    return PostgresJdbcConfig(
        host=args.postgres_host,
        port=args.postgres_port,
        database=args.postgres_db,
        user=args.postgres_user,
        password=args.postgres_password,
    )


def configure_object_storage(builder: Any, warehouse: WarehouseConfig) -> Any:
    """Add MinIO (S3A) and the PostgreSQL JDBC driver to a SparkSession builder."""

    return (
        builder.config("spark.jars.packages", f"{HADOOP_AWS_PACKAGE},{POSTGRESQL_JDBC_PACKAGE}")
        .config("spark.hadoop.fs.s3a.endpoint", warehouse.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", warehouse.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", warehouse.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )


def configure_iceberg_catalog(
    builder: Any,
    iceberg: IcebergCatalogConfig,
    postgres: PostgresJdbcConfig,
) -> Any:
    """Register an Iceberg catalog on a SparkSession builder.

    `catalog_type="jdbc"` (every real job) reuses the same PostgreSQL
    instance `configure_object_storage` already wires the JDBC driver for --
    Iceberg's `JdbcCatalog` auto-creates its own `iceberg_tables` /
    `iceberg_namespace_properties` bookkeeping tables there, separate from
    the `bronze`/`silver`/`gold` serving schemas. `io-impl` reuses
    HadoopFileIO so table data reads/writes go through the same
    `spark.hadoop.fs.s3a.*` settings already configured for MinIO, instead of
    pulling in a second S3 client via iceberg-aws-bundle.

    `catalog_type="hadoop"` (unit tests only) stores catalog metadata
    directly under `warehouse_uri` with no database involved, so tests can
    exercise real Iceberg reads/writes against a local `file://` path without
    a running Postgres.

    Call this *after* `configure_object_storage` on the same builder: both
    set `spark.jars.packages`, and this one's value (which repeats the
    hadoop-aws/postgresql packages alongside the new Iceberg one) is the
    complete list that should win.
    """

    iceberg.validate()
    catalog_prefix = f"spark.sql.catalog.{iceberg.catalog_name}"
    packages = ",".join((HADOOP_AWS_PACKAGE, POSTGRESQL_JDBC_PACKAGE, ICEBERG_SPARK_PACKAGE))
    builder = (
        builder.config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog_prefix}.warehouse", iceberg.warehouse_uri)
    )

    if iceberg.catalog_type == "hadoop":
        return builder.config(f"{catalog_prefix}.type", "hadoop")

    return (
        builder.config(f"{catalog_prefix}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{catalog_prefix}.uri", postgres.url)
        .config(f"{catalog_prefix}.jdbc.user", postgres.user)
        .config(f"{catalog_prefix}.jdbc.password", postgres.password)
        .config(f"{catalog_prefix}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config(f"{catalog_prefix}.jdbc.schema-version", "V1")
    )


def write_iceberg_table(
    dataframe: Any,
    table: str,
    partition_columns: Sequence[str],
    mode: str,
) -> None:
    """Write a Spark DataFrame into an Iceberg table, creating it if needed.

    `mode="overwrite"` matches every job's current default full-refresh
    behavior: `createOrReplace()` creates the table on the first run and
    replaces its data (keeping the Iceberg table identity/history) on every
    later run, in one call. `mode="append"` requires the table to already
    exist, so it is created empty first when missing.
    """

    writer = dataframe.writeTo(table).using("iceberg")
    if partition_columns:
        writer = writer.partitionedBy(*partition_columns)

    if mode == "overwrite":
        writer.createOrReplace()
        return

    if mode == "append":
        spark = dataframe.sparkSession
        if not spark.catalog.tableExists(table):
            writer.create()
        else:
            writer.append()
        return

    raise ValueError(f"Unsupported Iceberg write mode: {mode!r}")


def write_jdbc_table(dataframe: Any, table: str, postgres: PostgresJdbcConfig, mode: str) -> None:
    """Write a Spark DataFrame directly into one PostgreSQL table over JDBC.

    `mode="overwrite"` truncates instead of dropping the table (`truncate`
    option) so the hand-authored DDL -- constraints, foreign keys, the SCD2
    partial-unique indexes -- survives instead of being replaced by a
    Spark-inferred schema. Tables with foreign keys between them should be
    truncated together first with `truncate_tables_cascade` and written here
    with `mode="append"` instead, since PostgreSQL's single-table TRUNCATE
    fails when other selected tables still hold referencing rows.
    """

    (
        dataframe.write.format("jdbc")
        .option("url", postgres.url)
        .option("dbtable", table)
        .option("user", postgres.user)
        .option("password", postgres.password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode(mode)
        .save()
    )


def truncate_tables_cascade(spark: Any, tables: Sequence[str], postgres: PostgresJdbcConfig) -> None:
    """Truncate several PostgreSQL tables together through Spark's own JDBC driver.

    A single multi-table `TRUNCATE ... CASCADE` lets PostgreSQL resolve
    foreign-key dependency order itself -- something a per-table Spark JDBC
    overwrite cannot do. This uses the Postgres JDBC driver already loaded
    onto the Spark session's JVM (see `configure_object_storage`) through the
    session's own gateway, so no separate Python database client is involved.
    """

    if not tables:
        return

    quoted_tables = ", ".join('"' + table.replace(".", '"."') + '"' for table in tables)
    jvm = spark.sparkContext._jvm
    # The postgresql jar arrives via `spark.jars.packages` (Ivy) on the
    # current thread's *context* classloader. `DriverManager.getConnection`
    # additionally filters candidate drivers by the *caller's* classloader
    # (py4j's, here), which can't see a driver loaded that way even after
    # `Class.forName` registers it -- so this instantiates the driver
    # directly and calls `connect()` on it, bypassing that visibility check
    # entirely instead of going through `DriverManager`.
    driver_class = jvm.java.lang.Class.forName(
        "org.postgresql.Driver", True, jvm.Thread.currentThread().getContextClassLoader()
    )
    driver = driver_class.newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", postgres.user)
    properties.setProperty("password", postgres.password)
    connection = driver.connect(postgres.url, properties)
    try:
        statement = connection.createStatement()
        try:
            statement.execute(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")
        finally:
            statement.close()
    finally:
        connection.close()


def _env_value(name: str, default: str) -> str:
    """Read an environment variable with a default."""

    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable with a default."""

    raw_value = os.environ.get(name)
    return int(raw_value) if raw_value else default
