"""Feast entities, sources, and feature views over the FraudStream lakehouse."""

from __future__ import annotations

from datetime import timedelta

from feast import Entity, FeatureView, Field, PushSource, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.types import Bool, Float64, Int64, String

CUSTOMER_ROLLING_TTL = timedelta(days=31)
CUSTOMER_ORDERS_90D_TTL = timedelta(days=91)
MERCHANT_RISK_TTL = timedelta(days=31)
STREAM_FEATURE_TTL = timedelta(hours=1)


customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
    description="A generated cardholder; the natural id shared by Gold snapshots and Flink windows.",
)

merchant = Entity(
    name="merchant",
    join_keys=["merchant_id"],
    value_type=ValueType.STRING,
    description="A generated merchant; `merchant_dim_id` in Gold, `merchant_id` in the stream.",
)


customer_rolling_source = SparkSource(
    name="customer_rolling_source",
    query="""
        SELECT
            customer_id,
            event_timestamp + INTERVAL 1 DAY AS event_timestamp,
            created,
            txn_count_7d,
            txn_count_30d,
            CAST(amount_sum_7d AS DOUBLE) AS amount_sum_7d,
            CAST(amount_sum_30d AS DOUBLE) AS amount_sum_30d,
            CAST(amount_avg_7d AS DOUBLE) AS amount_avg_7d,
            CAST(amount_avg_30d AS DOUBLE) AS amount_avg_30d,
            distinct_merchant_count_7d,
            distinct_merchant_count_30d,
            declined_txn_count_7d,
            fraud_txn_count_30d
        FROM iceberg.gold.feat_customer_rolling
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="Rolling 7/30-day customer velocity snapshots, timestamped when they become knowable.",
)

customer_orders_90d_source = SparkSource(
    name="customer_orders_90d_source",
    query="""
        SELECT
            customer_id,
            event_timestamp + INTERVAL 1 DAY AS event_timestamp,
            created,
            total_orders_90d
        FROM iceberg.gold.feat_customer_total_orders_90d
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="90-day customer order counts.",
)

merchant_risk_source = SparkSource(
    name="merchant_risk_source",
    query="""
        SELECT
            merchant_dim_id AS merchant_id,
            event_timestamp + INTERVAL 1 DAY AS event_timestamp,
            created,
            merchant_category,
            merchant_txn_count_1d,
            merchant_txn_count_7d,
            merchant_txn_count_30d,
            CAST(merchant_amount_sum_30d AS DOUBLE) AS merchant_amount_sum_30d,
            CAST(merchant_amount_avg_30d AS DOUBLE) AS merchant_amount_avg_30d,
            merchant_distinct_customer_count_1d,
            merchant_declined_txn_count_1d,
            CAST(merchant_fraud_rate_1d AS DOUBLE) AS merchant_fraud_rate_1d,
            CAST(merchant_prior_fraud_rate_30d AS DOUBLE) AS merchant_prior_fraud_rate_30d,
            CAST(merchant_burst_ratio_1d_to_prior_30d AS DOUBLE) AS merchant_burst_ratio_1d_to_prior_30d,
            CAST(merchant_vs_category_amount_ratio_30d AS DOUBLE) AS merchant_vs_category_amount_ratio_30d
        FROM iceberg.gold.feat_merchant_risk_rolling
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="Merchant burst, historical risk, and category-relative snapshots.",
)


customer_rolling_features = FeatureView(
    name="customer_rolling_features",
    entities=[customer],
    ttl=CUSTOMER_ROLLING_TTL,
    schema=[
        Field(name="txn_count_7d", dtype=Int64),
        Field(name="txn_count_30d", dtype=Int64),
        Field(name="amount_sum_7d", dtype=Float64),
        Field(name="amount_sum_30d", dtype=Float64),
        Field(name="amount_avg_7d", dtype=Float64),
        Field(name="amount_avg_30d", dtype=Float64),
        Field(name="distinct_merchant_count_7d", dtype=Int64),
        Field(name="distinct_merchant_count_30d", dtype=Int64),
        Field(name="declined_txn_count_7d", dtype=Int64),
        Field(name="fraud_txn_count_30d", dtype=Int64),
    ],
    source=customer_rolling_source,
    online=True,
    tags={"layer": "gold", "grain": "customer-day", "widest_window_days": "30"},
)

customer_orders_90d_features = FeatureView(
    name="customer_orders_90d_features",
    entities=[customer],
    ttl=CUSTOMER_ORDERS_90D_TTL,
    schema=[Field(name="total_orders_90d", dtype=Int64)],
    source=customer_orders_90d_source,
    online=True,
    tags={"layer": "gold", "grain": "customer-day", "widest_window_days": "90"},
)

merchant_risk_features = FeatureView(
    name="merchant_risk_features",
    entities=[merchant],
    ttl=MERCHANT_RISK_TTL,
    schema=[
        Field(name="merchant_category", dtype=String),
        Field(name="merchant_txn_count_1d", dtype=Int64),
        Field(name="merchant_txn_count_7d", dtype=Int64),
        Field(name="merchant_txn_count_30d", dtype=Int64),
        Field(name="merchant_amount_sum_30d", dtype=Float64),
        Field(name="merchant_amount_avg_30d", dtype=Float64),
        Field(name="merchant_distinct_customer_count_1d", dtype=Int64),
        Field(name="merchant_declined_txn_count_1d", dtype=Int64),
        Field(name="merchant_fraud_rate_1d", dtype=Float64),
        Field(name="merchant_prior_fraud_rate_30d", dtype=Float64),
        Field(name="merchant_burst_ratio_1d_to_prior_30d", dtype=Float64),
        Field(name="merchant_vs_category_amount_ratio_30d", dtype=Float64),
    ],
    source=merchant_risk_source,
    online=True,
    tags={"layer": "gold", "grain": "merchant-day", "widest_window_days": "30"},
)


# Feast's Spark offline store writes only through `offline_write_batch`,
# which requires a `path=` source and a file-level format (parquet/csv/json/
# avro) -- an Iceberg table can never be its write target. The stream-push
# job therefore writes the offline side (these landing tables) with Spark
# directly, using Feast only for the online (Redis) side of the push. These
# sources exist for historical retrieval reads, which `table=` serves fine.
customer_features_5m_push = PushSource(
    name="customer_features_5m_push",
    batch_source=SparkSource(
        name="customer_features_5m_batch_source",
        table="iceberg.feature_store.stream_customer_features_5m",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        description="Append-only landing table for pushed 5-minute customer windows.",
    ),
    description="Flink's 5-minute customer windows, pushed by the stream-push deployment job.",
)

merchant_features_5m_push = PushSource(
    name="merchant_features_5m_push",
    batch_source=SparkSource(
        name="merchant_features_5m_batch_source",
        table="iceberg.feature_store.stream_merchant_features_5m",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        description="Append-only landing table for pushed 5-minute merchant windows.",
    ),
    description="Flink's 5-minute merchant windows, pushed by the stream-push deployment job.",
)

customer_features_5m_stream = FeatureView(
    name="customer_features_5m_stream",
    entities=[customer],
    ttl=STREAM_FEATURE_TTL,
    schema=[
        Field(name="txn_count", dtype=Int64),
        Field(name="amount_sum", dtype=Float64),
        Field(name="amount_avg", dtype=Float64),
        Field(name="amount_max", dtype=Float64),
        Field(name="declined_txn_count", dtype=Int64),
        Field(name="distinct_merchant_count", dtype=Int64),
        Field(name="distinct_device_count", dtype=Int64),
        Field(name="is_correction", dtype=Bool),
    ],
    source=customer_features_5m_push,
    online=True,
    tags={"layer": "streaming", "grain": "customer-5m-window"},
)

merchant_features_5m_stream = FeatureView(
    name="merchant_features_5m_stream",
    entities=[merchant],
    ttl=STREAM_FEATURE_TTL,
    schema=[
        Field(name="txn_count", dtype=Int64),
        Field(name="amount_sum", dtype=Float64),
        Field(name="amount_avg", dtype=Float64),
        Field(name="amount_max", dtype=Float64),
        Field(name="declined_txn_count", dtype=Int64),
        Field(name="distinct_customer_count", dtype=Int64),
        Field(name="merchant_category", dtype=String),
        Field(name="is_correction", dtype=Bool),
    ],
    source=merchant_features_5m_push,
    online=True,
    tags={"layer": "streaming", "grain": "merchant-5m-window"},
)
