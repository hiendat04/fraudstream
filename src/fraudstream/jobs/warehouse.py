"""What every Spark job shares: MinIO, the Iceberg catalog, and the PostgreSQL copy."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any, Sequence


DEFAULT_WAREHOUSE_URI = "s3a://fraudstream/warehouse"
DEFAULT_MINIO_ENDPOINT = "http://localhost:19000"

DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DATABASE = "fraudstream"

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"
POSTGRESQL_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"
ICEBERG_SPARK_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

DEFAULT_ICEBERG_CATALOG_NAME = "iceberg"
DEFAULT_ICEBERG_CATALOG_TYPE = "jdbc"
SUPPORTED_ICEBERG_CATALOG_TYPES = {"jdbc", "hadoop"}


@dataclass(frozen=True)
class WarehouseConfig:
    """Where MinIO is and how to log in. Empty keys are for local `file://` runs."""

    uri: str = DEFAULT_WAREHOUSE_URI
    endpoint: str = DEFAULT_MINIO_ENDPOINT
    access_key: str = ""
    secret_key: str = ""

    @classmethod
    def from_env(cls) -> "WarehouseConfig":
        """Read the MinIO settings from the environment, for code without a command line, such as an Airflow callable."""

        return cls(
            uri=_env_value("FRAUDSTREAM_WAREHOUSE_URI", DEFAULT_WAREHOUSE_URI),
            endpoint=_env_value("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
            access_key=required_env("MINIO_ACCESS_KEY"),
            secret_key=required_env("MINIO_SECRET_KEY"),
        )


@dataclass(frozen=True)
class IcebergCatalogConfig:
    """The Iceberg catalog: "jdbc" keeps its table list in PostgreSQL, "hadoop" (tests only) in files."""

    catalog_name: str = DEFAULT_ICEBERG_CATALOG_NAME
    catalog_type: str = DEFAULT_ICEBERG_CATALOG_TYPE
    warehouse_uri: str = DEFAULT_WAREHOUSE_URI

    def validate(self) -> None:
        """Reject an unknown catalog type."""

        if self.catalog_type not in SUPPORTED_ICEBERG_CATALOG_TYPES:
            allowed = ", ".join(sorted(SUPPORTED_ICEBERG_CATALOG_TYPES))
            raise ValueError(f"catalog_type must be one of: {allowed}")


@dataclass(frozen=True)
class PostgresJdbcConfig:
    """How Spark reaches PostgreSQL."""

    host: str = DEFAULT_POSTGRES_HOST
    port: int = DEFAULT_POSTGRES_PORT
    database: str = DEFAULT_POSTGRES_DATABASE
    user: str = ""
    password: str = ""

    @property
    def url(self) -> str:
        """The JDBC URL."""

        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


def warehouse_path(uri: str, *parts: str) -> str:
    """Join a URI and path parts. Not `pathlib.Path`, which turns `s3a://` into `s3a:/`."""

    return "/".join((uri.rstrip("/"), *parts))


def add_warehouse_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the MinIO flags to a job's parser."""

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
    _add_secret_argument(parser, "--minio-access-key", "MINIO_ACCESS_KEY")
    _add_secret_argument(parser, "--minio-secret-key", "MINIO_SECRET_KEY")


def warehouse_config_from_args(args: argparse.Namespace) -> WarehouseConfig:
    """The MinIO settings from parsed flags."""

    return WarehouseConfig(
        uri=args.warehouse_uri,
        endpoint=args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
    )


def add_iceberg_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the Iceberg catalog flags to a job's parser."""

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
    """The Iceberg settings from parsed flags."""

    return IcebergCatalogConfig(
        catalog_name=args.iceberg_catalog_name,
        catalog_type=args.iceberg_catalog_type,
        warehouse_uri=args.iceberg_warehouse_uri,
    )


def add_postgres_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the PostgreSQL flags to a job's parser."""

    parser.add_argument("--postgres-host", default=_env_value("POSTGRES_HOST", DEFAULT_POSTGRES_HOST))
    parser.add_argument("--postgres-port", type=int, default=_env_int("POSTGRES_PORT", DEFAULT_POSTGRES_PORT))
    parser.add_argument("--postgres-db", default=_env_value("POSTGRES_DB", DEFAULT_POSTGRES_DATABASE))
    _add_secret_argument(parser, "--postgres-user", "POSTGRES_USER")
    _add_secret_argument(parser, "--postgres-password", "POSTGRES_PASSWORD")


def postgres_config_from_args(args: argparse.Namespace) -> PostgresJdbcConfig:
    """The PostgreSQL settings from parsed flags."""

    return PostgresJdbcConfig(
        host=args.postgres_host,
        port=args.postgres_port,
        database=args.postgres_db,
        user=args.postgres_user,
        password=args.postgres_password,
    )


def configure_object_storage(builder: Any, warehouse: WarehouseConfig) -> Any:
    """Point Spark at MinIO and load the PostgreSQL driver."""

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
    """Register the Iceberg catalog on a Spark builder.

    Call it after `configure_object_storage`: its package list includes theirs and must win.
    HadoopFileIO makes Iceberg reuse the S3A settings for MinIO instead of a second S3 client.
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
    """Write rows to an Iceberg table, creating it on the first run.

    "overwrite" replaces the data and keeps the table's history; "append" adds to it.
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


def postgres_window(dataframe: Any, date_column: str, days: int) -> Any:
    """Keep the newest `days` days of `date_column`; 0 or less keeps every row."""

    if days <= 0:
        return dataframe

    from pyspark.sql import functions as F

    row_date = F.to_date(F.col(date_column))
    newest = dataframe.agg(F.max(row_date)).first()[0]
    if newest is None:
        return dataframe.limit(0)
    return dataframe.where(row_date > F.date_sub(F.lit(newest), days))


def write_postgres_copy(rows: Any, table: str, postgres: PostgresJdbcConfig, mode: str = "overwrite") -> int:
    """Write the PostgreSQL copy and return its row count."""

    write_jdbc_table(rows, table, postgres, mode=mode)
    return rows.count()


def write_jdbc_table(dataframe: Any, table: str, postgres: PostgresJdbcConfig, mode: str) -> None:
    """Write rows into a PostgreSQL table over JDBC.

    "overwrite" truncates rather than drops, so the table's keys and indexes survive.
    Tables linked by foreign keys: `truncate_tables_cascade` them first, then append.
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
    """Empty several PostgreSQL tables in one statement, so their foreign keys don't block it."""

    if not tables:
        return

    quoted_tables = ", ".join('"' + table.replace(".", '"."') + '"' for table in tables)
    jvm = spark.sparkContext._jvm
    # DriverManager can't see a driver Spark loaded at run time, so connect through the driver itself.
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


def required_env(name: str) -> str:
    """A credential from the environment, or an error."""

    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _add_secret_argument(parser: argparse.ArgumentParser, flag: str, name: str) -> None:
    """A credential flag: from the environment when set, otherwise required."""

    value = os.environ.get(name)
    parser.add_argument(flag, default=value or None, required=not value, help=f"Defaults to ${name}.")


def _env_value(name: str, default: str) -> str:
    """An environment variable, or the default."""

    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    """An integer environment variable, or the default."""

    raw_value = os.environ.get(name)
    return int(raw_value) if raw_value else default
