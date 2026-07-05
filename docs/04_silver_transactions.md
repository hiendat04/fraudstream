# Silver Transaction Design

This document defines the target Silver transaction table for the offline Spark
path. Silver reads `bronze.raw_transactions`, cleans source values into typed
business columns, deduplicates transaction records, and keeps enough lineage to
trace every clean row back to Bronze.

## Core Principle

Silver should answer one question:

```text
What is the clean, typed, one-row-per-transaction view of the source data?
```

Bronze preserves raw behavior. Silver changes business values intentionally:
it parses timestamps and amounts, standardizes strings, handles duplicates, and
flags records that are not reliable enough for analytics or model features.

Use `event_time` for business time. It is parsed from Bronze `event_timestamp`
and should drive event-date partitioning, late-arrival analysis, feature windows,
and downstream time-based joins. Bronze `created_ts` becomes `source_created_at`
and represents source arrival or creation time, not business time.

## Source And Target

Source table:

```text
bronze.raw_transactions
```

Target table:

```text
silver.transactions
```

Recommended local output path:

```text
data/silver/transactions/
```

Recommended storage format:

```text
Parquet
```

Recommended partition column:

```text
event_date
```

`event_date` should be derived from `event_time`, not from Bronze partition
paths. This keeps Silver aligned to business event time even when records arrive
late.

## Cleaned Transaction Schema

| Field | Type | Nullable | Source | Cleaning Rule |
|---|---|---:|---|---|
| `transaction_id` | `STRING` | No | `transaction_id` | Trim. Required deduplication key. |
| `account_id` | `STRING` | No | `account_id` | Trim. Blank values fail minimum quality. |
| `customer_id` | `STRING` | No | `customer_id` | Trim. Blank values fail minimum quality. |
| `merchant_id` | `STRING` | Yes | `merchant_id` | Trim. Convert blank to `NULL`. |
| `merchant_category` | `STRING` | Yes | `merchant_category` | Trim and lowercase. Keep snake-case category values. |
| `amount` | `DECIMAL(18,2)` | No | `amount` | Trim and cast. Non-numeric or negative values fail minimum quality. |
| `currency` | `STRING` | No | `currency` | Trim and uppercase. Expected value is `USD` for generated data. |
| `city` | `STRING` | Yes | `city` | Trim, collapse repeated spaces, and title-case. Convert blank to `NULL`. |
| `channel` | `STRING` | No | `channel` | Trim and lowercase. Expected values: `card_present`, `online`, `mobile_wallet`, `atm`. |
| `transaction_status` | `STRING` | No | `transaction_status` | Trim and lowercase. Expected values: `approved`, `declined`, `reversed`. |
| `is_fraud` | `BOOLEAN` | No | `is_fraud` | Cast `1` to `true`, `0` to `false`; other values fail minimum quality. |
| `event_time` | `TIMESTAMP` | No | `event_timestamp` | Parse source business timestamp. This is the canonical business time. |
| `event_date` | `DATE` | No | `event_time` | Date derived from `event_time`; used for partitioning. |
| `source_created_at` | `TIMESTAMP` | Yes | `created_ts` | Parse source creation or arrival timestamp. Use for late-arrival checks. |
| `arrival_delay_minutes` | `DOUBLE` | Yes | `event_time`, `source_created_at` | Difference between `source_created_at` and `event_time`. |
| `device_id` | `STRING` | Yes | `device_id` | Trim. Convert blank to `NULL`. Missing for `v1` rows is expected. |
| `ip_address` | `STRING` | Yes | `ip_address` | Trim. Convert blank to `NULL`. |
| `authentication_method` | `STRING` | Yes | `authentication_method` | Trim and lowercase. Convert blank to `NULL`. |
| `risk_signal_version` | `STRING` | Yes | `risk_signal_version` | Trim. Convert blank to `NULL`. |

## Quality And Lineage Fields

Silver should keep operational fields that explain how the record was selected
and whether it is safe for downstream use.

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `quality_status` | `STRING` | No | `valid`, `warning`, or `quarantined`. |
| `quality_issue_codes` | `ARRAY<STRING>` | No | Machine-readable issue codes found while cleaning. |
| `duplicate_record_count` | `INT` | No | Number of Bronze rows seen for the same `transaction_id`. |
| `dedup_rank` | `INT` | No | Selected row rank after applying deduplication rules. Main Silver rows should have rank `1`. |
| `_bronze_ingest_run_id` | `STRING` | Yes | Copied from Bronze `_ingest_run_id`. |
| `_bronze_source_file_path` | `STRING` | Yes | Copied from Bronze `_source_file_path`. |
| `_bronze_source_row_number` | `LONG` | Yes | Copied from Bronze `_source_row_number`. |
| `_bronze_raw_record_hash` | `STRING` | No | Copied from Bronze `_raw_record_hash` for audit checks. |
| `_silver_processed_at` | `TIMESTAMP` | No | Timestamp when the Silver job processed the row. |

## Standardization Rules

Silver standardization must be deterministic and easy to test:

- Trim leading and trailing whitespace from string fields.
- Convert blank strings to `NULL` except required identifiers and enum fields,
  which should fail quality checks when blank.
- Normalize `currency` to uppercase.
- Normalize `merchant_category`, `channel`, `transaction_status`, and
  `authentication_method` to lowercase.
- Normalize `city` by trimming, collapsing repeated internal spaces, and
  title-casing.
- Parse `amount` to `DECIMAL(18,2)`.
- Parse `event_timestamp` to `event_time`.
- Parse `created_ts` to `source_created_at`.
- Derive `event_date` from `event_time`.

Do not use `created_ts` as business time. It is useful for measuring late
arrivals, but event-time windows and feature logic should use `event_time`.

## Deduplication Rules

Silver should produce one main row per `transaction_id`.

Deduplicate with a Spark window over `transaction_id`. Rank candidate Bronze
rows in this order:

1. Rows that pass minimum quality checks before rows that fail.
2. Rows with parseable `event_time`, `amount`, and `is_fraud`.
3. Latest `source_created_at`, because later source arrivals may represent a
   correction or replay.
4. Latest Bronze `_ingested_at`.
5. Highest Bronze `_source_row_number`.
6. `_bronze_raw_record_hash` as a deterministic final tie-breaker.

Keep the row with `dedup_rank = 1` in `silver.transactions`. Preserve duplicate
evidence by storing `duplicate_record_count` on the selected row. A future audit
table can keep non-selected duplicates under:

```text
silver.transaction_duplicates
```

## Minimum Quality Rules

Rows can be cleaned only if the minimum business contract is satisfied.

| Rule | Quality Code | Main Table Behavior |
|---|---|---|
| Missing or blank `transaction_id` | `missing_transaction_id` | Quarantine. |
| Missing or blank `account_id` | `missing_account_id` | Quarantine. |
| Missing or blank `customer_id` | `missing_customer_id` | Quarantine. |
| `amount` cannot be cast to decimal | `invalid_amount` | Quarantine. |
| `amount` is negative | `negative_amount` | Quarantine. |
| `event_timestamp` cannot be parsed | `invalid_event_time` | Quarantine. |
| `is_fraud` is not `0` or `1` | `invalid_fraud_label` | Quarantine. |
| `currency` is not `USD` | `unexpected_currency` | Warning. |
| `channel` is outside the expected set | `unexpected_channel` | Warning. |
| `transaction_status` is outside the expected set | `unexpected_status` | Warning. |
| `source_created_at` is before `event_time` | `negative_arrival_delay` | Warning. |
| `arrival_delay_minutes` is greater than 60 | `late_arrival` | Warning. |
| `device_id` or `ip_address` is missing on `v2` rows | `missing_evolved_value` | Warning. |

`silver.transactions` should contain valid and warning rows after
deduplication. Quarantined rows should be written to a separate table so they
remain inspectable without polluting clean analytics:

```text
silver.transaction_quality_issues
```

## Spark Table Definition

```sql
CREATE TABLE IF NOT EXISTS silver.transactions (
  transaction_id STRING NOT NULL,
  account_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  merchant_id STRING,
  merchant_category STRING,
  amount DECIMAL(18,2) NOT NULL,
  currency STRING NOT NULL,
  city STRING,
  channel STRING NOT NULL,
  transaction_status STRING NOT NULL,
  is_fraud BOOLEAN NOT NULL,
  event_time TIMESTAMP NOT NULL,
  source_created_at TIMESTAMP,
  arrival_delay_minutes DOUBLE,
  device_id STRING,
  ip_address STRING,
  authentication_method STRING,
  risk_signal_version STRING,
  quality_status STRING NOT NULL,
  quality_issue_codes ARRAY<STRING> NOT NULL,
  duplicate_record_count INT NOT NULL,
  dedup_rank INT NOT NULL,
  _bronze_ingest_run_id STRING,
  _bronze_source_file_path STRING,
  _bronze_source_row_number BIGINT,
  _bronze_raw_record_hash STRING NOT NULL,
  _silver_processed_at TIMESTAMP NOT NULL,
  event_date DATE NOT NULL
)
USING PARQUET
PARTITIONED BY (event_date);
```

## Validation Expectations

The Silver job should prove it cleaned Bronze without hiding important data
quality behavior.

| Check | Expected Result |
|---|---|
| Deduplication | `silver.transactions` has one row per non-quarantined `transaction_id`. |
| Row accounting | Bronze rows equal selected Silver rows plus duplicate rows plus quarantined rows. |
| Type casting | `amount`, `is_fraud`, `event_time`, and `source_created_at` use typed columns. |
| Business time | `event_date` is derived from `event_time`. |
| Standardized strings | `currency`, `city`, `channel`, and `transaction_status` match standardization rules. |
| Late arrivals | Late records remain available with `late_arrival` in `quality_issue_codes`. |
| Schema evolution | `v1` missing evolved fields remain `NULL`; `v2` missing evolved values are flagged. |
| Lineage | Every Silver row keeps Bronze source path, row number, ingest run, and raw record hash. |

These expectations should become unit tests and a reusable validation command
when the Silver Spark job is implemented.
