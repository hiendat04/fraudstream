# Bronze Ingestion Design

This document defines the Bronze transaction table for the offline Spark path.
Bronze is the first lakehouse layer after raw source files. Its job is to
preserve source behavior, add ingestion metadata, and write queryable Parquet
without cleaning business values.

## Core Principle

Bronze should answer one question:

```text
What did the source system send, where did it come from, and when did we ingest it?
```

Bronze must not deduplicate, standardize, enrich, or repair records. Those
changes belong in Silver. This means Bronze intentionally keeps:

- duplicate `transaction_id` values
- late arrivals where `created_ts` is much later than `event_timestamp`
- uppercase, lowercase, padded, or blank source values
- old `v1` rows that do not have evolved columns
- new `v2` rows with device, IP, authentication, and risk signal fields

## Source Input

Default raw source path:

```text
data/raw_source/offline_transactions/
```

Source layout:

```text
schema_version=v1/transaction_date=YYYY-MM-DD/transactions.csv
schema_version=v2/transaction_date=YYYY-MM-DD/transactions.csv
```

The Bronze Spark job should read `_manifest.json` for file discovery when
possible. The manifest records the generated files and source generation config.

## Target Table

Recommended table name:

```text
bronze.raw_transactions
```

Recommended local output path:

```text
data/bronze/raw_transactions/
```

Future object-storage path:

```text
s3a://fraudstream/bronze/raw_transactions/
```

Recommended storage format:

```text
Parquet
```

## Raw Transaction Fields

All source business columns should be loaded as nullable `STRING` values in
Bronze. This is deliberate. Bronze preserves raw text exactly; Silver will cast
amounts, timestamps, booleans, and dates into analytical types.

| Field | Type | Nullable | Source Version | Meaning |
|---|---|---:|---|---|
| `transaction_id` | `STRING` | Yes | `v1+` | Source transaction identifier. Duplicates are allowed in Bronze. |
| `account_id` | `STRING` | Yes | `v1+` | Account identifier tied to the customer. |
| `customer_id` | `STRING` | Yes | `v1+` | Customer identifier used for joins and feature grouping. |
| `merchant_id` | `STRING` | Yes | `v1+` | Merchant identifier. May be blank in raw source rows. |
| `merchant_category` | `STRING` | Yes | `v1+` | Merchant category such as grocery, travel, or online marketplace. |
| `amount` | `STRING` | Yes | `v1+` | Raw source amount. Silver should cast to decimal. |
| `currency` | `STRING` | Yes | `v1+` | Raw currency value. May contain inconsistent casing. |
| `city` | `STRING` | Yes | `v1+` | Raw transaction city. May contain blanks, padding, or casing issues. |
| `channel` | `STRING` | Yes | `v1+` | Transaction channel such as online, card present, wallet, or ATM. |
| `transaction_status` | `STRING` | Yes | `v1+` | Raw transaction status. May contain inconsistent casing. |
| `is_fraud` | `STRING` | Yes | `v1+` | Raw fraud label as source text. Silver should cast to boolean/integer. |
| `event_timestamp` | `STRING` | Yes | `v1+` | Raw business event time. Silver should parse for event-time logic. |
| `created_ts` | `STRING` | Yes | `v1+` | Raw source creation or arrival time. Used later for late-arrival analysis. |

## Nullable Evolved Columns

These columns exist in the Bronze table even when reading old `v1` files. For
`v1` source partitions, populate them as `NULL` because the columns did not
exist yet. For `v2` partitions, keep the source value exactly as read, including
blank strings where the source emitted blanks.

| Field | Type | Nullable | Source Version | Meaning |
|---|---|---:|---|---|
| `device_id` | `STRING` | Yes | `v2` | Device identifier added after the schema change date. |
| `ip_address` | `STRING` | Yes | `v2` | IP address added for device and fraud-ring analysis. |
| `authentication_method` | `STRING` | Yes | `v2` | Authentication method such as 3DS, OTP, biometric, none, chip, pin, or tap. |
| `risk_signal_version` | `STRING` | Yes | `v2` | Source risk signal version. Current generated value is `v2`. |

This schema design makes `v1` and `v2` files unionable while preserving the
fact that older partitions genuinely did not have the evolved fields.

## Source Metadata Fields

Metadata fields explain how each Bronze row was ingested. They are not business
facts and should use a leading underscore.

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `_source_system` | `STRING` | No | Producing system name. Use `fraudstream_generator`. |
| `_source_dataset` | `STRING` | No | Source dataset name. Use `offline_transactions`. |
| `_source_file_path` | `STRING` | No | Full path of the CSV file that produced the row. |
| `_source_file_name` | `STRING` | No | File name, currently `transactions.csv`. |
| `_source_row_number` | `LONG` | Yes | Row number inside the source file when available. Useful for traceability. |
| `_source_manifest_path` | `STRING` | Yes | Manifest file used for discovery, usually `_manifest.json`. |
| `_source_manifest_created_at` | `STRING` | Yes | `created_at` value copied from the manifest. Keep as raw manifest text. |
| `_ingest_run_id` | `STRING` | No | Unique ID for one Spark ingestion run. |
| `_ingested_at` | `TIMESTAMP` | No | Timestamp when Spark wrote the Bronze row. |
| `_raw_record_hash` | `STRING` | No | Stable hash of raw source column values for duplicate and audit checks. |
| `_corrupt_record` | `STRING` | Yes | Raw malformed line if CSV parsing fails in permissive mode. |

`_raw_record_hash` must not replace source keys. It is only an audit helper.
Duplicates should still be visible as repeated `transaction_id` values.

## Partition Columns

Bronze should use low-cardinality, source-aligned partitions:

| Partition Column | Type | Source | Meaning |
|---|---|---|---|
| `ingest_date` | `STRING` | Spark ingestion run | Date the Bronze job loaded the data, formatted as `YYYY-MM-DD`. |
| `schema_version` | `STRING` | Source path | Source schema version, currently `v1` or `v2`. |
| `transaction_date` | `STRING` | Source path | Business transaction date from the raw partition path. |

Recommended layout:

```text
data/bronze/raw_transactions/
`-- ingest_date=YYYY-MM-DD/
    `-- schema_version=v1/
        `-- transaction_date=YYYY-MM-DD/
            `-- part-*.parquet
```

Partition values should be derived from the source path and ingestion context,
not from cleaned business logic.

## Spark Table Definition

The first Spark implementation should use an explicit schema and disable schema
inference. This keeps the Bronze contract stable even when source values are
messy.

```sql
CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
  transaction_id STRING,
  account_id STRING,
  customer_id STRING,
  merchant_id STRING,
  merchant_category STRING,
  amount STRING,
  currency STRING,
  city STRING,
  channel STRING,
  transaction_status STRING,
  is_fraud STRING,
  event_timestamp STRING,
  created_ts STRING,
  device_id STRING,
  ip_address STRING,
  authentication_method STRING,
  risk_signal_version STRING,
  _source_system STRING,
  _source_dataset STRING,
  _source_file_path STRING,
  _source_file_name STRING,
  _source_row_number BIGINT,
  _source_manifest_path STRING,
  _source_manifest_created_at STRING,
  _ingest_run_id STRING,
  _ingested_at TIMESTAMP,
  _raw_record_hash STRING,
  _corrupt_record STRING,
  ingest_date STRING,
  schema_version STRING,
  transaction_date STRING
)
USING PARQUET
PARTITIONED BY (ingest_date, schema_version, transaction_date);
```

## Ingestion Rules

| Concern | Bronze Rule |
|---|---|
| Deduplication | Do not deduplicate. Preserve repeated rows exactly. |
| Missing values | Preserve source blanks. Only use `NULL` for columns absent from old schema versions or parser-level missing fields. |
| Format issues | Do not trim, uppercase, lowercase, or normalize fields. |
| Type casting | Keep source business columns as `STRING`. Cast in Silver. |
| Schema evolution | Add evolved columns to the table as nullable fields. Fill `v1` rows with `NULL` for evolved columns. |
| File traceability | Capture source file path, row number when available, manifest path, and ingest run ID. |
| Write mode | Prefer append for ingestion runs. Use controlled overwrite only for local regeneration. |
| Corrupt records | Keep malformed lines in `_corrupt_record` for inspection instead of silently dropping them. |

## Bronze To Silver Boundary

Silver is responsible for changing business meaning. Bronze only preserves and
records source behavior.

Silver should later:

- deduplicate by `transaction_id`
- parse `amount` to decimal
- parse `event_timestamp` and `created_ts` to timestamp
- cast `is_fraud` to a typed label
- trim and standardize `city`, `currency`, and `transaction_status`
- handle missing `merchant_id`, `device_id`, `ip_address`, and authentication values
- enforce a clean, stable schema for analytics and ML features

## Validation Expectations

The Bronze ingestion job should prove that it preserved the raw source:

| Check | Expected Result |
|---|---|
| Row count | Bronze row count equals source row count after duplicate injection from `_quality_summary.json`. |
| Duplicate preservation | Duplicate `transaction_id` count remains greater than zero. |
| Schema evolution | `v1` partitions have `NULL` evolved columns; `v2` partitions include evolved columns. |
| Raw formatting | Rows with padded cities, lowercase currency, or uppercase status still exist in Bronze. |
| Late arrivals | Rows where `created_ts` is more than 60 minutes after `event_timestamp` still exist. |
| Partition coverage | Distinct `schema_version` and `transaction_date` values match raw source partitions. |
| Metadata coverage | `_source_file_path`, `_ingest_run_id`, `_ingested_at`, and `_raw_record_hash` are populated. |

These checks are the acceptance criteria for the first Bronze Spark job.
