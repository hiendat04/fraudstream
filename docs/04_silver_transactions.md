# Silver Transactions

Silver reads Bronze and answers: *what is the clean, typed, one-row-per-transaction
view of the data?* It parses amounts and timestamps, standardises strings,
deduplicates, and flags rows that aren't reliable, all with lineage back to Bronze.
Nothing is dropped silently: rows that don't make the main table go to a quality
table.

**`event_time` is the business time**, parsed from Bronze `event_timestamp`. It drives
partitioning, feature windows and joins. `created_ts` becomes `source_created_at`,
which only measures late arrival.

## Tables

| Table | Holds | Partitioned by |
|---|---|---|
| `iceberg.silver.stg_transactions` | One clean row per `transaction_id` (valid and warning rows) | `event_date`, taken from `event_time`, not from Bronze paths |
| `iceberg.silver.stg_transaction_quality_issues` | Quarantined, duplicate-rejected and warning rows | `quality_status` |

Both are also written to PostgreSQL as `silver.stg_transactions` and
`silver.stg_transaction_quality_issues`. Data files sit under
`s3a://fraudstream/warehouse/silver/`, managed by Iceberg (see
[15_lakehouse_iceberg.md](15_lakehouse_iceberg.md)).

## Run it

```bash
PYTHONPATH=src python -m fraudstream.jobs.bronze.ingest_transactions       # Bronze first
PYTHONPATH=src python -m fraudstream.jobs.silver.transactions \
  --output-dir data/silver/transactions \
  --quality-output-dir data/silver/transaction_quality_issues \
  --write-mode overwrite
```

Add `--skip-postgres-write` for Iceberg only, or `--spark-ui --spark-ui-retain-seconds 300`
to inspect the plan at `http://localhost:4040`. Each run also writes
`_silver_transactions_summary.json` and `_silver_quality_report.json` under
`data/silver/transactions/`. The report counts rows by action and by issue code, so
it is the first place to look when counts don't match. Tuning results are in
[optimization/spark/silver_job_optimization.md](optimization/spark/silver_job_optimization.md).

## Cleaned schema

| Field | Type | Rule |
|---|---|---|
| `transaction_id` | `STRING`, required | Trimmed; the dedup key |
| `account_id`, `customer_id` | `STRING`, required | Trimmed; blank fails |
| `merchant_id` | `STRING` | Trimmed; blank becomes `NULL` |
| `merchant_category` | `STRING` | Trimmed, lowercased, spaces and hyphens become `_`; blank `NULL` |
| `amount` | `DECIMAL(18,2)`, required | Cast; non-numeric or negative fails |
| `currency` | `STRING`, required | Trimmed, uppercased; expected `USD` |
| `city` | `STRING` | Trimmed, spaces collapsed, title-cased; blank `NULL` |
| `channel` | `STRING`, required | Standardised; `card_present`, `online`, `mobile_wallet`, `atm` |
| `transaction_status` | `STRING`, required | Standardised; `approved`, `declined`, `reversed` |
| `is_fraud` | `BOOLEAN`, required | `1` becomes true, `0` false; anything else fails |
| `event_time` | `TIMESTAMP`, required | Parsed `event_timestamp` |
| `event_date` | `DATE`, required | Derived from `event_time` |
| `source_created_at` | `TIMESTAMP` | Parsed `created_ts`; unparseable becomes `NULL` |
| `arrival_delay_minutes` | `DOUBLE` | `source_created_at` minus `event_time` |
| `device_id`, `ip_address`, `authentication_method`, `risk_signal_version` | `STRING` | Trimmed; blank `NULL`; missing on `v1` is expected |

Every row also carries:

| Field | Meaning |
|---|---|
| `quality_status` | `valid`, `warning` or `quarantined` |
| `quality_issue_codes` | `ARRAY<STRING>` of issue codes found |
| `duplicate_record_count`, `dedup_rank` | How many Bronze rows shared the ID, and this row's rank (main rows are rank `1`) |
| `_bronze_ingest_run_id`, `_bronze_source_file_path`, `_bronze_source_row_number`, `_bronze_raw_record_hash` | Lineage back to Bronze |
| `_silver_processed_at` | When Silver processed it |

The quality table adds `_silver_record_action` (`selected`, `duplicate_rejected` or
`quarantined`) and `_silver_quality_reported_at`.

## Quality rules

| Problem | Code | Result |
|---|---|---|
| Missing or blank `transaction_id`, `account_id`, `customer_id` | `missing_transaction_id`, `missing_account_id`, `missing_customer_id` | Quarantine |
| `amount` not a number | `invalid_amount` | Quarantine |
| Negative `amount` | `negative_amount` | Quarantine |
| `event_timestamp` unparseable | `invalid_event_time` | Quarantine |
| `is_fraud` not `0` or `1` | `invalid_fraud_label` | Quarantine |
| `currency` not `USD` | `unexpected_currency` | Warning |
| `channel` or `transaction_status` outside the expected set | `unexpected_channel`, `unexpected_status` | Warning |
| `source_created_at` before `event_time` | `negative_arrival_delay` | Warning |
| Arrival delay over 60 minutes | `late_arrival` | Warning |
| `device_id` or `ip_address` missing on a `v2` row | `missing_evolved_value` | Warning |

The main table holds valid and warning rows after deduplication. Late records stay in
it, tagged `late_arrival`.

## Deduplication

A Spark window over `transaction_id` ranks the candidates, best first:

1. rows that pass the minimum quality checks
2. rows with a parseable `event_time`, `amount` and `is_fraud`
3. latest `source_created_at` (a later arrival may be a correction)
4. latest Bronze `_ingested_at`
5. highest Bronze `_source_row_number`
6. `_bronze_raw_record_hash`, as a deterministic final tie-breaker

Rank `1` goes to the main table. The rest go to the quality table as
`duplicate_rejected`.

## What "correct" looks like

| Check | Expected |
|---|---|
| One row per ID | The main table has one row per non-quarantined `transaction_id` |
| Row accounting | Bronze rows = selected + duplicate-rejected + quarantined |
| Types | Amount, fraud label, `event_time` and `source_created_at` are typed columns |
| Business time | `event_date` comes from `event_time` |
| Strings | Standardised as above; nullable fields stay `NULL` |
| Evidence | Every quarantined, rejected and warning row is in the quality table |
| Lineage | Every row keeps its Bronze path, row number, run and hash |

```bash
PYTHONPATH=src python -m unittest tests.unit.test_silver_transactions
```
