"""Bridge the Flink DataStream feature topology into shared Iceberg tables.

PyFlink's DataStream API has no native Iceberg sink -- Iceberg's officially
supported Flink integration is the Table API/SQL connector. This module
converts the JSON-string streams `runtime.py` already builds (clean
transactions, customer/merchant features) into typed rows, registers the same
Iceberg catalog Spark's batch jobs use, and inserts into three Iceberg tables
declared with a primary key so the sink runs in upsert mode -- a late
correction for a window that already emitted a feature row replaces it
instead of duplicating it.

This module is imported only by the Python 3.12 Flink environment, the same
boundary `runtime.py` and `transactions.py` already keep.
"""

from __future__ import annotations

import json
from typing import Any

from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from fraudstream.jobs.flink.transactions import StreamingFeatureConfig
from fraudstream.jobs.warehouse import IcebergCatalogConfig, PostgresJdbcConfig, WarehouseConfig


CLEAN_TRANSACTIONS_TABLE = "clean_transactions"
CUSTOMER_FEATURES_TABLE = "customer_features_5m"
MERCHANT_FEATURES_TABLE = "merchant_features_5m"
ICEBERG_NAMESPACE = "streaming"

CLEAN_TRANSACTIONS_ROW_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "transaction_id",
        "account_id",
        "customer_id",
        "merchant_id",
        "merchant_category",
        "amount",
        "amount_cents",
        "currency",
        "city",
        "channel",
        "transaction_status",
        "evaluation_is_fraud",
        "event_timestamp",
        "event_timestamp_ms",
        "produced_at",
        "arrival_delay_seconds",
        "device_id",
        "ip_address",
        "authentication_method",
        "schema_version",
        "problem_flags",
        "source_topic",
        "source_partition",
        "source_sequence",
        "partition_key",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.BOOLEAN(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LIST(Types.STRING()),
        Types.STRING(),
        Types.INT(),
        Types.LONG(),
        Types.STRING(),
    ],
)

_FEATURE_BASE_FIELDS = [
    "feature_id",
    "feature_type",
    "entity_type",
    "entity_id",
    "window_start",
    "window_end",
    "window_size_minutes",
    "txn_count",
    "amount_sum",
    "amount_avg",
    "amount_max",
    "declined_txn_count",
    "last_event_timestamp",
    "is_correction",
    "emitted_at",
]
_FEATURE_BASE_TYPES = [
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.INT(),
    Types.LONG(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.LONG(),
    Types.STRING(),
    Types.BOOLEAN(),
    Types.STRING(),
]

CUSTOMER_FEATURES_ROW_TYPE = Types.ROW_NAMED(
    [*_FEATURE_BASE_FIELDS, "customer_id", "distinct_merchant_count", "distinct_device_count"],
    [*_FEATURE_BASE_TYPES, Types.STRING(), Types.LONG(), Types.LONG()],
)

MERCHANT_FEATURES_ROW_TYPE = Types.ROW_NAMED(
    [*_FEATURE_BASE_FIELDS, "merchant_id", "merchant_category", "distinct_customer_count"],
    [*_FEATURE_BASE_TYPES, Types.STRING(), Types.STRING(), Types.LONG()],
)


def clean_transaction_to_row(payload: str) -> Row:
    """Parse one clean-transaction JSON payload into a typed Iceberg row."""

    event = json.loads(payload)
    return Row(
        event_id=event["event_id"],
        transaction_id=event["transaction_id"],
        account_id=event["account_id"],
        customer_id=event["customer_id"],
        merchant_id=event["merchant_id"],
        merchant_category=event["merchant_category"],
        amount=event["amount"],
        amount_cents=event["amount_cents"],
        currency=event["currency"],
        city=event.get("city"),
        channel=event["channel"],
        transaction_status=event["transaction_status"],
        evaluation_is_fraud=event["evaluation_is_fraud"],
        event_timestamp=event["event_timestamp"],
        event_timestamp_ms=event["event_timestamp_ms"],
        produced_at=event["produced_at"],
        arrival_delay_seconds=event["arrival_delay_seconds"],
        device_id=event["device_id"],
        ip_address=event["ip_address"],
        authentication_method=event.get("authentication_method"),
        schema_version=event["schema_version"],
        problem_flags=list(event.get("problem_flags", [])),
        source_topic=event["source_topic"],
        source_partition=event["source_partition"],
        source_sequence=event["source_sequence"],
        partition_key=event["partition_key"],
    )


def customer_feature_to_row(payload: str) -> Row:
    """Parse one customer feature JSON payload into a typed Iceberg row."""

    feature = json.loads(payload)
    return Row(**_feature_base_kwargs(feature), **{
        "customer_id": feature["customer_id"],
        "distinct_merchant_count": feature["distinct_merchant_count"],
        "distinct_device_count": feature["distinct_device_count"],
    })


def merchant_feature_to_row(payload: str) -> Row:
    """Parse one merchant feature JSON payload into a typed Iceberg row."""

    feature = json.loads(payload)
    return Row(**_feature_base_kwargs(feature), **{
        "merchant_id": feature["merchant_id"],
        "merchant_category": feature.get("merchant_category"),
        "distinct_customer_count": feature["distinct_customer_count"],
    })


def _feature_base_kwargs(feature: dict[str, Any]) -> dict[str, Any]:
    """Return the shared feature-record fields common to both entity types."""

    return {name: feature[name] for name in _FEATURE_BASE_FIELDS}


def attach_iceberg_sinks(
    environment: StreamExecutionEnvironment,
    deduplicated_events: Any,
    customer_features: Any,
    merchant_features: Any,
    config: StreamingFeatureConfig,
) -> None:
    """Register the Iceberg catalog and insert the three streams into it.

    Uses a `StreamStatementSet` (`create_statement_set()` +
    `attach_as_datastream()`) so the Table API inserts become part of the
    same job graph as the existing Kafka `DataStream` sinks in `runtime.py`,
    all executed together by the single `environment.execute(...)` call in
    `execute_streaming_feature_job`, instead of starting a second Flink job.
    """

    table_environment = StreamTableEnvironment.create(environment)
    table_environment.execute_sql(_catalog_ddl(config.iceberg, config.postgres, config.warehouse))
    table_environment.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog_name}.{ICEBERG_NAMESPACE}"
    )
    table_environment.execute_sql(_clean_transactions_ddl(config.iceberg))
    table_environment.execute_sql(_customer_features_ddl(config.iceberg))
    table_environment.execute_sql(_merchant_features_ddl(config.iceberg))

    statement_set = table_environment.create_statement_set()
    statement_set.add_insert(
        _table_path(config.iceberg, CLEAN_TRANSACTIONS_TABLE),
        table_environment.from_data_stream(
            deduplicated_events.map(clean_transaction_to_row, output_type=CLEAN_TRANSACTIONS_ROW_TYPE)
        ),
    )
    statement_set.add_insert(
        _table_path(config.iceberg, CUSTOMER_FEATURES_TABLE),
        table_environment.from_data_stream(
            customer_features.map(customer_feature_to_row, output_type=CUSTOMER_FEATURES_ROW_TYPE)
        ),
    )
    statement_set.add_insert(
        _table_path(config.iceberg, MERCHANT_FEATURES_TABLE),
        table_environment.from_data_stream(
            merchant_features.map(merchant_feature_to_row, output_type=MERCHANT_FEATURES_ROW_TYPE)
        ),
    )
    statement_set.attach_as_datastream()


def _table_path(iceberg: IcebergCatalogConfig, table_name: str) -> str:
    """Return the fully-qualified `catalog.namespace.table` SQL identifier."""

    return f"{iceberg.catalog_name}.{ICEBERG_NAMESPACE}.{table_name}"


def _catalog_ddl(iceberg: IcebergCatalogConfig, postgres: PostgresJdbcConfig, warehouse: WarehouseConfig) -> str:
    """Build the `CREATE CATALOG` statement for the configured catalog type.

    S3A properties are passed through as generic catalog properties -- the
    Flink Iceberg catalog factory forwards any property it doesn't recognize
    directly to the Hadoop `Configuration` backing `HadoopFileIO`, the same
    io-impl Spark's catalog uses. This mirrors `configure_object_storage` /
    `configure_iceberg_catalog` in `fraudstream.jobs.warehouse` exactly so
    both engines read and write the identical MinIO location.
    """

    common_properties = {
        "type": "iceberg",
        "warehouse": iceberg.warehouse_uri,
        "io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "fs.s3a.endpoint": warehouse.endpoint,
        "fs.s3a.access.key": warehouse.access_key,
        "fs.s3a.secret.key": warehouse.secret_key,
        "fs.s3a.path.style.access": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.connection.ssl.enabled": "false",
    }
    if iceberg.catalog_type == "hadoop":
        properties = {**common_properties, "catalog-type": "hadoop"}
    else:
        # Iceberg's Flink `FlinkCatalogFactory` only recognizes
        # `catalog-type` values of hive/hadoop/rest -- unlike Spark's
        # `SparkCatalog`, it has no built-in "jdbc" shorthand. `catalog-impl`
        # (the fully-qualified class name) is the documented escape hatch
        # for catalog implementations outside that fixed list, and Flink's
        # factory checks it before falling back to `catalog-type`.
        properties = {
            **common_properties,
            "catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
            "uri": postgres.url,
            "jdbc.user": postgres.user,
            "jdbc.password": postgres.password,
            # Auto-migrates the JDBC catalog's bookkeeping tables to the
            # view-support schema; without it, JdbcCatalog throws
            # UnsupportedOperationException the first time view support is
            # probed. Must match the Spark side (configure_iceberg_catalog)
            # since both engines share the same catalog tables.
            "jdbc.schema-version": "V1",
        }
    properties_sql = ",\n  ".join(f"'{key}' = '{value}'" for key, value in properties.items())
    return f"CREATE CATALOG {iceberg.catalog_name} WITH (\n  {properties_sql}\n)"


def _clean_transactions_ddl(iceberg: IcebergCatalogConfig) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {_table_path(iceberg, CLEAN_TRANSACTIONS_TABLE)} (
  event_id STRING,
  transaction_id STRING,
  account_id STRING,
  customer_id STRING,
  merchant_id STRING,
  merchant_category STRING,
  amount STRING,
  amount_cents BIGINT,
  currency STRING,
  city STRING,
  channel STRING,
  transaction_status STRING,
  evaluation_is_fraud BOOLEAN,
  event_timestamp STRING,
  event_timestamp_ms BIGINT,
  produced_at STRING,
  arrival_delay_seconds BIGINT,
  device_id STRING,
  ip_address STRING,
  authentication_method STRING,
  schema_version STRING,
  problem_flags ARRAY<STRING>,
  source_topic STRING,
  source_partition INT,
  source_sequence BIGINT,
  partition_key STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
)
"""


def _customer_features_ddl(iceberg: IcebergCatalogConfig) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {_table_path(iceberg, CUSTOMER_FEATURES_TABLE)} (
  {_FEATURE_BASE_DDL_COLUMNS}
  customer_id STRING,
  distinct_merchant_count BIGINT,
  distinct_device_count BIGINT,
  PRIMARY KEY (feature_id) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
)
"""


def _merchant_features_ddl(iceberg: IcebergCatalogConfig) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {_table_path(iceberg, MERCHANT_FEATURES_TABLE)} (
  {_FEATURE_BASE_DDL_COLUMNS}
  merchant_id STRING,
  merchant_category STRING,
  distinct_customer_count BIGINT,
  PRIMARY KEY (feature_id) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
)
"""


_FEATURE_BASE_DDL_COLUMNS = """feature_id STRING,
  feature_type STRING,
  entity_type STRING,
  entity_id STRING,
  window_start STRING,
  window_end STRING,
  window_size_minutes INT,
  txn_count BIGINT,
  amount_sum STRING,
  amount_avg STRING,
  amount_max STRING,
  declined_txn_count BIGINT,
  last_event_timestamp STRING,
  is_correction BOOLEAN,
  emitted_at STRING,
  """
