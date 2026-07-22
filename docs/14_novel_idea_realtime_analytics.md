# Novel Idea: Real-Time Fraud Analytics

## Motivation

The Flink pipeline produces clean transactions, five-minute features, late
events, and fraud alerts as Kafka topics. Kafka is effective for transporting
these records, but it is not intended for historical analytical queries or
interactive investigation.

The proposed extension adds a real-time analytics layer:

- **ClickHouse** stores and aggregates high-volume streaming results.
- **Grafana** presents operational and fraud-analysis dashboards from
  ClickHouse.

This complements the existing PostgreSQL serving layer. PostgreSQL continues to
serve batch Bronze, Silver, Gold, and offline feature tables, while ClickHouse
serves recent streaming analytics.

## Proposed Data Flow

```text
Kafka transactions
        |
        v
      Flink
        |
        v
Kafka feature, alert, and audit topics
        |
        v
   ClickHouse
        |
        v
     Grafana
```

ClickHouse would consume these existing Flink output topics:

- `financial_transactions_clean`
- `financial_transactions_late`
- `fraud_features_customer_5m`
- `fraud_features_merchant_5m`
- `fraud_alerts`

Kafka-engine tables would receive the messages. Incremental materialized views
would validate and transform each message into persistent `MergeTree` tables.
Additional materialized views would maintain query-ready minute and hourly
aggregates.

## Proposed Data Products

| Data product | Purpose |
|---|---|
| Real-time transaction history | Investigate recent customer and merchant activity |
| Customer feature history | Track customer velocity and amount changes by window |
| Merchant feature history | Track merchant burst and risk signals by window |
| Fraud alert history | Search alerts by time, customer, merchant, and alert type |
| Late-event history | Measure event-time reliability and investigate delayed records |

Grafana would expose the most useful operational views:

- transactions and fraud alerts per minute;
- p95 event-to-alert latency;
- late- and duplicate-event rates;
- highest-risk customers and merchants;
- fraud and alert distribution by city and merchant category.

## Engineering Value

This extension introduces a new real-time OLAP and visualization stack without
replacing the current pipelines. It demonstrates:

- direct Kafka-to-analytical-database ingestion;
- columnar table, partition, and sorting-key design;
- incremental aggregation with materialized views;
- retention management for high-volume event data;
- dashboard design for streaming data products;
- analytical query and ingestion-performance measurement.

A useful performance study would compare aggregating raw event rows at query
time with reading pre-aggregated materialized-view tables. The comparison should
report measured query duration, rows and bytes read, memory use, and dashboard
refresh latency. No performance improvement should be claimed without captured
results.

## Design Boundaries

The proposal does not introduce a fraud model or scoring API. It also does not
replace Flink, Kafka, PostgreSQL, Airflow, or DataHub. Its responsibility is
limited to persisting, querying, and visualizing the streaming outputs that the
current platform already produces.

## References

- [ClickHouse Kafka table engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka)
- [ClickHouse incremental materialized views](https://clickhouse.com/docs/materialized-view/incremental-materialized-view)
- [Grafana ClickHouse data source](https://grafana.com/docs/plugins/grafana-clickhouse-datasource/latest/)
