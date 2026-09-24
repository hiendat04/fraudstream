# Bronze Ingestion

Bronze is the first lakehouse layer. It answers one question: *what did the source
send, where did it come from, and when did we ingest it?* It keeps everything as the
source wrote it and adds ingestion metadata. Iceberg catalog details are in
[15_lakehouse_iceberg.md](15_lakehouse_iceberg.md).

**Bronze never cleans.** It keeps duplicate `transaction_id`s, late arrivals,
padded, mixed-case and blank values, `v1` rows without the newer columns, and `v2`
rows with them. Fixing those is Silver's job.

## Where things live

| What | Where |
|---|---|
| Source CSVs | `s3a://fraudstream/raw/offline_transactions/` (found through `_manifest.json`, or by listing `--source-uri`) |
| Bronze table | `iceberg.bronze.raw_transactions`, data in MinIO under `s3a://fraudstream/warehouse/bronze/raw_transactions/`, catalog in PostgreSQL |
| Copies in PostgreSQL | `bronze.raw_transactions` and `bronze.raw_transaction_ingest_runs`, one row per source record and per run |
| Job summary | `data/bronze/raw_transactions/_bronze_ingestion_summary.json` |

Always read and write through the table name, not the file path: Iceberg owns the layout.

## Run it

```bash
uv sync --extra spark                       # PySpark needs Java 17+
PYTHONPATH=src python -m fraudstream.generators.offline_transactions
PYTHONPATH=src python -m fraudstream.jobs.bronze.ingest_transactions \
  --source-dir data/raw_source/offline_transactions \
  --source-uri s3a://fraudstream/raw/offline_transactions \
  --output-dir data/bronze/raw_transactions \
  --warehouse-uri s3a://fraudstream/warehouse \
  --write-mode overwrite
```

MinIO and PostgreSQL must be running (see the README Quick Start). Use
`--skip-postgres-write` for the Iceberg table alone, `--write-mode append` to keep
several runs, and `--spark-ui --spark-ui-retain-seconds 300` to keep the Spark UI
(`http://localhost:4040`) open for screenshots.

Then check it against the source:

```bash
PYTHONPATH=src python -m fraudstream.jobs.bronze.validate_transactions \
  --source-dir data/raw_source/offline_transactions \
  --source-uri s3a://fraudstream/raw/offline_transactions \
  --warehouse-uri s3a://fraudstream/warehouse \
  --report-path data/bronze/raw_transactions/_bronze_validation_summary.json
```

It prints a JSON report and exits non-zero on a failed check.

## Columns

The schema is explicit, with no inference. Every business column is a nullable
`STRING`, because Bronze keeps raw text and Silver casts it.

| Group | Columns |
|---|---|
| Raw, `v1` and `v2` | `transaction_id`, `account_id`, `customer_id`, `merchant_id`, `merchant_category`, `amount`, `currency`, `city`, `channel`, `transaction_status`, `is_fraud`, `event_timestamp`, `created_ts` |
| Evolved, `v2` only | `device_id`, `ip_address`, `authentication_method`, `risk_signal_version` |
| Ingestion metadata | `_source_system`, `_source_dataset`, `_source_file_path`, `_source_file_name`, `_source_row_number`, `_source_manifest_path`, `_source_manifest_created_at`, `_ingest_run_id`, `_ingested_at`, `_raw_record_hash`, `_corrupt_record` |
| Partitions | `ingest_date`, `schema_version`, `transaction_date` |

- **Evolved columns** are `NULL` for `v1` files, where they did not exist. In `v2`, a
  blank stays a blank string. Files are read by their physical headers.
- **Metadata** columns start with `_`. `_ingested_at` is a `TIMESTAMP` and
  `_source_row_number` a `LONG`. `_raw_record_hash` is an audit helper only and never
  replaces the source key. Malformed lines go to `_corrupt_record` instead of being dropped.
- **Partitions** come from the source path and the run, never from cleaned values:
  `ingest_date=…/schema_version=…/transaction_date=…/part-*.parquet`.

## Rules

| Concern | Bronze does |
|---|---|
| Duplicates | Keeps them |
| Missing values | Keeps source blanks; `NULL` only for columns absent from an old schema |
| Formatting | No trimming or case changes |
| Types | Everything stays `STRING` |
| Traceability | Records file path, row number, manifest and run ID |
| Writes | Append for real runs, overwrite only to regenerate locally |

## Handing over to Silver

Silver deduplicates by `transaction_id`, casts amount, timestamps and the fraud label,
standardises `city`, `currency` and `transaction_status`, handles missing merchant,
device, IP and authentication values, and enforces a clean schema.

## What "correct" looks like

| Check | Expected |
|---|---|
| Row count | Equals the source count after duplicate injection |
| Duplicates | Repeated `transaction_id`s still exist |
| Schema evolution | `v1` rows have `NULL` evolved columns, `v2` rows have values |
| Raw formatting | Padded cities, lowercase currency and uppercase status still exist |
| Late arrivals | Rows with `created_ts` over 60 minutes after `event_timestamp` still exist |
| Coverage | Distinct `schema_version` and `transaction_date` match the source partitions |
| Metadata | `_source_file_path`, `_ingest_run_id`, `_ingested_at`, `_raw_record_hash` are filled |

```bash
PYTHONPATH=src python -m unittest \
  tests.unit.test_bronze_ingest_transactions tests.unit.test_bronze_validate_transactions
```
