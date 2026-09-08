# Offline Data Generator

This document explains the first FraudStream data generator: a Python tool that
creates raw offline financial transaction files with intentional data problems.
The output is designed to behave like data owned by another department, ready for
a later Bronze ingestion pipeline.

## Purpose

The generator produces source data that is realistic enough for local
Spark/Bronze/Silver exercises without making the first cleaning pipeline too
hard:

- raw transaction records are stored before any cleaning or deduplication
- skew is visible in business columns such as `city` and `merchant_category`
- high-cardinality identifiers are present for realistic joins and aggregations
- schema changes across time are preserved in separate source partitions
- duplicate records are injected so later pipelines can prove deduplication
- transaction timestamps follow peak-hour and burst-day traffic patterns
- some source records arrive late or out of event-time order
- a small number of fields are missing or formatted inconsistently
- fraud labels include rare, coordinated fraud-ring behavior through reused
  devices and IP addresses

## Run The Generator

From the repository root:

```bash
PYTHONPATH=src python -m fraudstream.generators.offline_transactions
```

You can also run the project entry point:

```bash
PYTHONPATH=src python main.py
```

To override the local evidence directory or the MinIO raw-data location:

```bash
PYTHONPATH=src python -m fraudstream.generators.offline_transactions \
  --output-dir /tmp/fraudstream_offline_transactions \
  --raw-uri s3a://fraudstream/raw/offline_transactions_dev
```

## Configuration

Default configuration lives in:

```text
configs/generator/offline_transactions.json
```

Important settings:

| Setting | Meaning |
|---|---|
| `n_transactions` | Number of unique base transactions before duplicates are injected. |
| `n_customers`, `n_accounts`, `n_merchants` | Controls ID cardinality. |
| `skew_city`, `skew_city_ratio` | Makes one city dominate the generated records. |
| `skew_merchant_category`, `skew_merchant_category_ratio` | Makes one merchant category dominate the generated records. |
| `duplicate_rate` | Controls repeated raw transaction rows. Default is `0.02`. |
| `late_arrival_rate` | Controls records where `created_ts` is much later than `event_timestamp`. |
| `missing_value_rate` | Controls light missingness in realistic raw fields such as city, merchant, device, IP, and authentication method. |
| `inconsistent_format_rate` | Controls easy-to-clean formatting issues such as padded city names, lowercase currency, and uppercase status. |
| `burst_day_count` | Number of dates with unusually heavy transaction volume. |
| `fraud_ring_count` | Number of reusable suspicious device/IP pairs used by a small fraud-ring scenario. |
| `schema_change_date` | Splits older `v1` source files from newer `v2` source files. |
| `output_dir` | Local directory for the manifest and quality-summary evidence files only (small files, not the raw CSVs). |
| `raw_uri` | MinIO (`s3a://`) URI where the actual partitioned raw CSV files are written. Defaults to `s3a://fraudstream/raw/offline_transactions`. |
| `drift_start_date` | Date (inside the history window) from which sampled `amount` values start ramping upward. `None`/omitted disables drift entirely. |
| `drift_amount_multiplier_end` | Amount multiplier reached by the end of the history window when drift is enabled. Ramps linearly from `1.0` at `drift_start_date`. |
| `labels_uri` | MinIO (`s3a://`) URI where the standalone `(transaction_id, is_fraud, event_timestamp)` training-label table is written. Defaults to `s3a://fraudstream/raw/transaction_labels`. |

> **Default config now enables drift.** `configs/generator/offline_transactions.json` ships
> with `drift_start_date: "2026-05-01"` and `drift_amount_multiplier_end: 1.6`, so a default
> `fraudstream-generate-offline` run now produces a different dataset than earlier runs of this
> generator: transaction amounts ramp up to 1.6x over the last ~60 days of the history window,
> the synthetic fraud rate is slightly higher inside that window (`amount` feeds
> `_sample_fraud_label`), and a new MinIO prefix (`labels_uri`) is deleted-then-rewritten on
> every run alongside `raw_uri`. Any evidence numbers captured from earlier default runs (row
> counts, skew, fraud rate) are stale and should be regenerated before being cited again.

## Implementation Coverage

| Capability | Implementation |
|---|---|
| Simulate skew | `city` is skewed toward `skew_city`; `merchant_category` is skewed toward `skew_merchant_category`. |
| Simulate high cardinality | Generates many unique `transaction_id`, `customer_id`, `account_id`, `merchant_id`, and `device_id` values. |
| Simulate schema evolution | Partitions before `schema_change_date` are `schema_version=v1`; partitions on or after that date are `schema_version=v2` with added columns. |
| Simulate another offline data problem | Repeats approximately `duplicate_rate` rows. The default config injects about 10,000 duplicates for 500,000 base transactions. |
| Simulate bursty and late data | Uses peak-hour traffic, burst dates, shuffled file order, and delayed `created_ts` values. |
| Simulate raw source messiness | Injects small, controlled rates of missing values and inconsistent formats. |
| Simulate fraud behavior | Creates rare labels with higher risk for high-value, online, cross-border, high-risk merchant, late-night, and fraud-ring activity. |
| Simulate data drift | From `drift_start_date` to the end of the history window, sampled `amount` values are scaled by a factor ramping linearly from `1.0` to `drift_amount_multiplier_end`. Off (`1.0` everywhere) when `drift_start_date` is `None`. |
| Use generator configuration | All core parameters are read from `configs/generator/offline_transactions.json`. |
| Store data for Bronze ingestion | Writes partitioned raw CSV files to MinIO (`raw_uri`) and `_manifest.json`/quality-summary evidence locally (`output_dir`). The manifest's `files` list contains the MinIO URI of every partition. |

## Output Layout

Raw CSV partitions go to MinIO, under the `raw_uri` prefix (default
`s3a://fraudstream/raw/offline_transactions`):

```text
s3a://fraudstream/raw/offline_transactions/
|-- schema_version=v1/
|   `-- transaction_date=YYYY-MM-DD/
|       `-- transactions.csv
`-- schema_version=v2/
    `-- transaction_date=YYYY-MM-DD/
        `-- transactions.csv
```

Small evidence files (manifest and quality summaries) stay local, under
`output_dir` (default `data/raw_source/offline_transactions/`):

```text
data/raw_source/offline_transactions/
|-- _manifest.json
|-- _quality_summary.csv
`-- _quality_summary.json
```

`schema_version=v1` files intentionally do not contain these evolved columns:

- `device_id`
- `ip_address`
- `authentication_method`
- `risk_signal_version`

`schema_version=v2` files include those columns. This gives the future Bronze and
Silver jobs a real schema evolution case to handle.

The standalone training-label table goes to MinIO, under its own `labels_uri` prefix
(default `s3a://fraudstream/raw/transaction_labels`), separate from `raw_uri`:

```text
s3a://fraudstream/raw/transaction_labels/
`-- transaction_labels.csv
```

It has three columns: `transaction_id`, `is_fraud`, `event_timestamp` -- the same names the
values carry in the transaction rows, so no mental mapping is needed when joining. It is
kept separate from the raw transaction partitions -- and from Gold's own `is_fraud` column
-- so that a later Feast feature view can carry features only, with the label joined in at
training time. Like `raw_uri`, this prefix is deleted and rewritten on every generator run.

## Bronze Ingestion Contract

The generated source files are intentionally raw. The future Bronze ingestion job
should preserve the records close to source and add ingestion metadata.

Recommended Bronze table:

```text
raw_transactions
```

Recommended ingestion behavior:

| Concern | Recommendation |
|---|---|
| File discovery | Read `_manifest.json` (local) for the list of MinIO source-file URIs, or list `raw_uri` directly in MinIO when no manifest is present. The manifest's top-level `label_file` key holds the URI of the standalone training-label table (see below); it is not one of the `raw_transactions` source files. |
| Dedup key | Use `transaction_id` to identify duplicate source records. |
| Partitions | Preserve or derive `schema_version` and `transaction_date`. |
| Schema evolution | Read `v1` and `v2` files with missing columns allowed. |
| Raw retention | Do not clean or deduplicate before Bronze; handle that in Silver. |

## Quality Evidence

The generator writes two summary files for project evidence and validation:

```text
data/raw_source/offline_transactions/_quality_summary.json
data/raw_source/offline_transactions/_quality_summary.csv
```

Evidence available in those files:

| Evidence | Example from default config |
|---|---|
| Data volume | `500000` base rows and about `510000` rows after duplicate injection. |
| Duplicate rate | Around `0.0196` after duplicates are included in total row count. |
| Skew distribution | `New York` and `online_marketplace` dominate their columns. |
| Burst traffic | A configured set of burst dates receives a larger share of traffic. |
| Late arrivals | Records where `created_ts` is more than 60 minutes after `event_timestamp`. |
| Raw quality issues | Counts of missing values and inconsistent formats. |
| Fraud scenario | Fraud rate, fraud row count, fraud-ring rows, and suspicious device reuse. |
| Cardinality | Distinct counts for transaction, customer, account, merchant, and device IDs. |
| Schema evolution | Row counts before and after `schema_change_date`. |
| Storage details | Data format, file count, and partition columns. |
| Data drift | The JSON summary's `drift` section reports `{"enabled": false}` when `drift_start_date` is unset, or the drift window, mean amount before/after `drift_start_date`, and the configured multiplier when enabled. The CSV summary carries the two mean-amount rows (`drift.mean_amount_before`, `drift.mean_amount_after`), blank when drift is disabled. |
| Label table | The JSON summary's `label_table` section reports the label table's MinIO URI, row count, and column names. The CSV summary carries `label_table.row_count`. |

## Validate Locally

Run the unit test:

```bash
PYTHONPATH=src python -m unittest tests.unit.test_offline_transactions
```

Run a syntax compile check:

```bash
PYTHONPATH=src python -m compileall -q src tests main.py
```

Both commands should complete without errors.
