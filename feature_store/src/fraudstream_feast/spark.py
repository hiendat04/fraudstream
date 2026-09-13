"""Spark configuration and Iceberg landing tables for the Feast feature store.

Feast's Spark offline store starts its own `SparkSession` from the
`spark_conf` block in `feature_store.yaml`. That session has to reach the same
MinIO bucket and the same Iceberg catalog the batch jobs write, so the
settings are derived here from the very same config dataclasses
`fraudstream.jobs.warehouse` uses, rather than hand-copied into YAML where
they could drift.
"""

from __future__ import annotations

import os
from typing import Any

from fraudstream.jobs.warehouse import (
    DEFAULT_POSTGRES_DATABASE,
    DEFAULT_POSTGRES_HOST,
    DEFAULT_POSTGRES_PASSWORD,
    DEFAULT_POSTGRES_PORT,
    DEFAULT_POSTGRES_USER,
    HADOOP_AWS_PACKAGE,
    ICEBERG_SPARK_PACKAGE,
    POSTGRESQL_JDBC_PACKAGE,
    IcebergCatalogConfig,
    PostgresJdbcConfig,
    WarehouseConfig,
)


ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
PUSH_NAMESPACE = "feature_store"

CUSTOMER_PUSH_TABLE_COLUMNS: tuple[str, ...] = (
    "customer_id",
    "event_timestamp",
    "created",
    "txn_count",
    "amount_sum",
    "amount_avg",
    "amount_max",
    "declined_txn_count",
    "distinct_merchant_count",
    "distinct_device_count",
    "is_correction",
)

MERCHANT_PUSH_TABLE_COLUMNS: tuple[str, ...] = (
    "merchant_id",
    "event_timestamp",
    "created",
    "txn_count",
    "amount_sum",
    "amount_avg",
    "amount_max",
    "declined_txn_count",
    "distinct_customer_count",
    "merchant_category",
    "is_correction",
)

_CUSTOMER_DDL = """
  customer_id STRING,
  event_timestamp TIMESTAMP,
  created TIMESTAMP,
  txn_count BIGINT,
  amount_sum DOUBLE,
  amount_avg DOUBLE,
  amount_max DOUBLE,
  declined_txn_count BIGINT,
  distinct_merchant_count BIGINT,
  distinct_device_count BIGINT,
  is_correction BOOLEAN
"""

_MERCHANT_DDL = """
  merchant_id STRING,
  event_timestamp TIMESTAMP,
  created TIMESTAMP,
  txn_count BIGINT,
  amount_sum DOUBLE,
  amount_avg DOUBLE,
  amount_max DOUBLE,
  declined_txn_count BIGINT,
  distinct_customer_count BIGINT,
  merchant_category STRING,
  is_correction BOOLEAN
"""


def feast_spark_conf(
    *,
    warehouse: WarehouseConfig,
    iceberg: IcebergCatalogConfig,
    postgres: PostgresJdbcConfig,
) -> dict[str, str]:
    """Return the Spark settings Feast needs to read and write the Iceberg lakehouse."""

    iceberg.validate()
    catalog = iceberg.catalog_name
    conf: dict[str, str] = {
        "spark.jars.packages": ",".join(
            (HADOOP_AWS_PACKAGE, POSTGRESQL_JDBC_PACKAGE, ICEBERG_SPARK_PACKAGE)
        ),
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.warehouse": iceberg.warehouse_uri,
        "spark.hadoop.fs.s3a.endpoint": warehouse.endpoint,
        "spark.hadoop.fs.s3a.access.key": warehouse.access_key,
        "spark.hadoop.fs.s3a.secret.key": warehouse.secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.sql.shuffle.partitions": "8",
    }
    if iceberg.catalog_type == "hadoop":
        conf[f"spark.sql.catalog.{catalog}.type"] = "hadoop"
    else:
        conf[f"spark.sql.catalog.{catalog}.catalog-impl"] = "org.apache.iceberg.jdbc.JdbcCatalog"
        conf[f"spark.sql.catalog.{catalog}.uri"] = postgres.url
        conf[f"spark.sql.catalog.{catalog}.jdbc.user"] = postgres.user
        conf[f"spark.sql.catalog.{catalog}.jdbc.password"] = postgres.password
        conf[f"spark.sql.catalog.{catalog}.io-impl"] = "org.apache.iceberg.hadoop.HadoopFileIO"
        conf[f"spark.sql.catalog.{catalog}.jdbc.schema-version"] = "V1"
    return conf


def configs_from_env() -> tuple[WarehouseConfig, IcebergCatalogConfig, PostgresJdbcConfig]:
    """Build all three warehouse configs from the container environment."""

    warehouse = WarehouseConfig.from_env()
    iceberg = IcebergCatalogConfig(
        catalog_name=os.environ.get("FRAUDSTREAM_ICEBERG_CATALOG_NAME", "iceberg"),
        catalog_type=os.environ.get("FRAUDSTREAM_ICEBERG_CATALOG_TYPE", "jdbc"),
        warehouse_uri=os.environ.get("FRAUDSTREAM_ICEBERG_WAREHOUSE_URI", warehouse.uri),
    )
    postgres = PostgresJdbcConfig(
        host=os.environ.get("POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
        port=int(os.environ.get("POSTGRES_PORT") or DEFAULT_POSTGRES_PORT),
        database=os.environ.get("POSTGRES_DB", DEFAULT_POSTGRES_DATABASE),
        user=os.environ.get("POSTGRES_USER", DEFAULT_POSTGRES_USER),
        password=os.environ.get("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
    )
    return warehouse, iceberg, postgres


def build_spark_session(
    *,
    warehouse: WarehouseConfig,
    iceberg: IcebergCatalogConfig,
    postgres: PostgresJdbcConfig,
    app_name: str = "FraudStreamFeast",
    master: str = "local[4]",
) -> Any:
    """Build a SparkSession with the same settings Feast's offline store uses."""

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in feast_spark_conf(
        warehouse=warehouse, iceberg=iceberg, postgres=postgres
    ).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def create_push_target_tables(spark: Any, *, catalog_name: str = "iceberg") -> tuple[str, str]:
    """Create the append-only Iceberg tables Feast pushes streaming features into."""

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{PUSH_NAMESPACE}")
    customer_table = f"{catalog_name}.{PUSH_NAMESPACE}.stream_customer_features_5m"
    merchant_table = f"{catalog_name}.{PUSH_NAMESPACE}.stream_merchant_features_5m"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {customer_table} ({_CUSTOMER_DDL}) "
        "USING iceberg PARTITIONED BY (days(event_timestamp))"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {merchant_table} ({_MERCHANT_DDL}) "
        "USING iceberg PARTITIONED BY (days(event_timestamp))"
    )
    return (customer_table, merchant_table)
