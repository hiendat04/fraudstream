# FraudStream: Real-Time Financial Transaction Intelligence Platform

FraudStream is a data engineering and MLOps project for building a production-style fraud analytics platform. It generates realistic financial transaction data, preserves raw source behavior, and prepares the foundation for Spark, Flink, Parquet lakehouse processing, feature engineering, and model operations.

The project is built around a practical idea: fraud detection depends on reliable data pipelines before any model can be trusted. Real transaction systems produce late records, duplicates, schema changes, traffic spikes, high-cardinality IDs, skewed entities, and messy source values. FraudStream turns those problems into controlled, reproducible engineering scenarios.

## What This Project Demonstrates

FraudStream focuses on the data platform skills behind fraud analytics:

- realistic batch and streaming transaction data generation
- source-style raw data preservation before cleaning
- Kafka replay for real-time processing development
- Spark-oriented offline processing path from raw files to Parquet
- Flink-oriented streaming path from Kafka events to event-time logic
- deliberate data quality issues for Bronze, Silver, and Gold layers
- reproducible configs, manifests, and summary artifacts for validation

Detailed generator behavior, schemas, and configuration notes live in the `docs/` directory so the README can stay focused on the project as a whole.

## Current Implementation

The repository currently includes:

| Component | Purpose | Details |
|---|---|---|
| Offline transaction generator | Creates partitioned raw CSV transaction extracts with realistic source problems | [docs/01_data_generator.md](docs/01_data_generator.md) |
| Streaming transaction generator | Creates a reproducible JSONL event log that behaves like a Kafka topic export | [docs/02_streaming_generator.md](docs/02_streaming_generator.md) |
| Kafka replay producer | Publishes generated stream events to Kafka for Flink processing | [docs/02_streaming_generator.md](docs/02_streaming_generator.md) |
| Flink streaming feature job | Validates and deduplicates Kafka events, uses a p95-measured watermark delay, computes event-time customer and merchant features, and emits alerts | [docs/07_flink_streaming_pipeline.md](docs/07_flink_streaming_pipeline.md) |
| Local Kafka stack | Runs Kafka, topic initialization, and Kafka UI with Docker Compose | [docker-compose.yml](docker-compose.yml) |
| Bronze transaction ingestion | Reads raw offline CSV partitions and writes metadata-rich Bronze Parquet | [docs/03_bronze_ingestion.md](docs/03_bronze_ingestion.md) |
| Silver transaction deduplication | Cleans typed transaction fields and writes one deterministic row per transaction ID | [docs/04_silver_transactions.md](docs/04_silver_transactions.md) |
| PostgreSQL serving schema | Creates Bronze, Silver, Gold, and metadata schemas for DBeaver and governance workflows | [docs/05_gold_tables.md](docs/05_gold_tables.md) |
| Fraud feature engineering design | Defines explainable, point-in-time-safe customer, merchant, amount, device/IP, and late-arrival features | [docs/06_feature_engineering.md](docs/06_feature_engineering.md) |
| Merchant risk features | Builds rolling merchant burst, historical fraud-rate, and merchant-category comparison signals | [docs/06_feature_engineering.md](docs/06_feature_engineering.md) |
| Airflow batch orchestration | Runs raw-to-Bronze, Bronze-to-Silver/Gold, and offline-feature DAGs with validation gates and asset dependencies | [docs/09_orchestration_flow.md](docs/09_orchestration_flow.md) |

Default configs generate more than 500,000 offline rows and more than 500,000 streaming records. Generated evidence files such as `_manifest.json`, `_quality_summary.json`, and `_stream_summary.json` capture row counts, quality issues, timing behavior, partitions, and output metadata.

## Architecture

FraudStream separates source simulation from processing layers. Raw CSV and JSONL files represent producer-owned source data. Spark will handle offline ingestion and Parquet transformation. Kafka and Flink support the streaming path.

```mermaid
flowchart LR
    generator[Data Generator]
    airflow[Airflow Batch DAGs]
    raw[Raw CSV and JSONL]
    kafka[Kafka]
    spark[Spark Jobs]
    flink[Flink Jobs]
    bronze[Bronze Parquet]
    silver[Silver Tables]
    gold[Gold Facts and Features]
    mlflow[MLflow]
    api[Fraud Scoring API]
    monitor[Monitoring]

    airflow --> generator
    airflow --> spark
    generator --> raw
    raw --> spark
    spark --> bronze
    bronze --> spark
    spark --> silver
    silver --> spark
    spark --> gold

    generator --> kafka
    kafka --> flink
    flink --> gold

    gold --> mlflow
    mlflow --> api
    api --> monitor
    gold --> monitor
```

The intended lakehouse flow is:

```text
raw source data -> Bronze Parquet -> Silver clean tables -> Gold features -> model and monitoring workflows
```

## Data Design

The offline path writes raw CSV partitions under:

```text
data/raw_source/offline_transactions/
```

These files simulate source-system extracts. They intentionally include duplicates, late arrivals, missing values, inconsistent formats, skew, high-cardinality IDs, fraud labels, and schema evolution.

The streaming path writes a local event log under:

```text
data/raw_stream/transactions/topic=financial_transactions/events.jsonl
```

That log can be replayed into Kafka topic `financial_transactions`. Each event keeps separate event time and production time fields so later Flink jobs can practice event-time windows, watermarks, deduplication, and late-event handling.

## Repository Structure

```text
financial-fraud-detection/
├── airflow/                  # Airflow DAGs, shared configuration, and local runtime
├── configs/generator/        # Generator runtime configs
├── data/                     # Generated local data outputs
├── docs/                     # Detailed implementation documentation
├── flink/                    # Isolated Python 3.12 PyFlink runtime and connector location
├── src/fraudstream/          # Python source code
│   ├── generators/           # Offline and streaming generators
│   ├── jobs/                 # Spark, Flink, and PostgreSQL data jobs
│   └── producers/            # Kafka replay producer
├── tests/unit/               # Unit tests
├── docker-compose.yml        # Local Kafka, PostgreSQL, and Airflow services
├── main.py
├── pyproject.toml
└── uv.lock
```

Generated data is reproducible output. Do not manually edit generated partitions or topic logs; update the generator or config and regenerate.

## Setup

FraudStream targets Python `>=3.14`.

Create and sync the local environment with `uv`:

```bash
uv sync
```

Install the optional Kafka dependency when using the replay producer:

```bash
uv sync --extra kafka
```

Install the optional Spark dependency when working on Bronze ingestion:

```bash
uv sync --extra spark
```

Install the optional PostgreSQL dependency when publishing Parquet outputs into
the local serving database:

```bash
uv sync --extra postgres
```

To keep Kafka, Spark, and PostgreSQL publishing extras installed locally:

```bash
uv sync --extra kafka --extra spark --extra postgres
```

Run commands from the repository root. During local development, commands use `PYTHONPATH=src` so the `fraudstream` package is importable without a full packaging workflow.

## Quick Start

Generate offline raw transaction data:

```bash
PYTHONPATH=src python -m fraudstream.generators.offline_transactions
```

Generate streaming event data:

```bash
PYTHONPATH=src python -m fraudstream.generators.streaming_transactions
```

Start local Kafka:

```bash
docker compose up -d kafka kafka-topic-init kafka-ui
```

Kafka listens on `localhost:9092`. Kafka UI is available at:

```text
http://localhost:18080
```

Start local PostgreSQL and create the serving schemas:

```bash
docker compose up -d postgres postgres-schema-init
```

PostgreSQL is available for DBeaver at `localhost:5432`, database
`fraudstream`, user `fraudstream`.

Replay generated events to Kafka:

```bash
PYTHONPATH=src python -m fraudstream.producers.stream_replay \
  --bootstrap-servers localhost:9092 \
  --topic financial_transactions \
  --events-per-second 5000
```

Verify local Spark execution:

```bash
PYTHONPATH=src python -m fraudstream.jobs.spark_local_check
```

Ingest raw offline CSV files into Bronze Parquet:

```bash
PYTHONPATH=src python -m fraudstream.jobs.bronze.ingest_transactions
```

Build deduplicated Silver transaction Parquet:

```bash
PYTHONPATH=src python -m fraudstream.jobs.silver.transactions
```

Build Gold transaction facts, dimensions, aggregates, and feature tables:

```bash
PYTHONPATH=src python -m fraudstream.jobs.gold.transactions
```

Run core Gold and offline features as separate orchestration boundaries:

```bash
PYTHONPATH=src python -m fraudstream.jobs.gold.transactions --core-only
PYTHONPATH=src python -m fraudstream.jobs.gold.offline_features
```

Start Airflow for the three batch DAGs:

```bash
docker compose --profile orchestration up --build -d \
  airflow-db airflow-init airflow-api-server \
  airflow-dag-processor airflow-scheduler
```

Open `http://localhost:18081`, unpause the three `fraudstream_*` DAGs, and
trigger `fraudstream_raw_to_bronze`. Validated asset events start the other DAGs
in order. See [docs/09_orchestration_flow.md](docs/09_orchestration_flow.md).

### Observe the offline pipeline in Spark UI

The raw-data generator is Python, so Spark UI begins at Bronze. Enable it on
any Bronze, Silver, or Gold Spark job with `--spark-ui`. The command prints the
actual URL chosen by Spark; it is normally `http://localhost:4040`.

For example, Silver provides the clearest view of how Spark handles the
deliberate offline data problems:

```bash
PYTHONPATH=src python -m fraudstream.jobs.silver.transactions \
  --spark-ui \
  --spark-ui-port 4040 \
  --spark-ui-retain-seconds 300
```

Open the printed URL while the command is running. The retention option keeps
the live UI open for five minutes after the final Spark action so there is time
to inspect and capture screenshots. It does not slow the transformations; it
only delays `spark.stop()` after processing finishes.

The Spark Jobs page uses readable FraudStream groups:

| Layer | What to capture in Spark UI |
|---|---|
| Bronze | Raw CSV scans, schema-version unions, source-lineage columns, Parquet partition writes, and duplicate profiling. |
| Silver | Type cleanup, late-arrival rules, quality classification, the shuffle/sort window used for deterministic deduplication, and quality-evidence writes. |
| Gold | Daily aggregations, rolling windows, point-in-time feature joins, merchant category broadcast joins, and adaptive skew handling. |

Use **Jobs** for the named business steps, **SQL/DataFrame** for physical query
plans, **Stages** for shuffle and task details, and **Storage** for the reused
Bronze, Silver, or Gold frames. Run the layers sequentially if they use the same
preferred UI port. If that port is busy, use the actual URL printed by Spark.

Publish Silver Parquet into PostgreSQL:

```bash
PYTHONPATH=src python -m fraudstream.jobs.postgres.publish --layer silver
```

Publish Gold Parquet into PostgreSQL:

```bash
PYTHONPATH=src python -m fraudstream.jobs.postgres.publish --layer gold
```

Stop the local Kafka stack:

```bash
docker compose down
```

## Validation

Run the current unit tests:

```bash
PYTHONPATH=src python -m unittest \
  tests.unit.test_offline_transactions \
  tests.unit.test_streaming_transactions \
  tests.unit.test_stream_replay \
  tests.unit.test_flink_transactions \
  tests.unit.test_flink_watermark_calibration \
  tests.unit.test_bronze_ingest_transactions \
  tests.unit.test_bronze_validate_transactions \
  tests.unit.test_silver_transactions \
  tests.unit.test_gold_transactions \
  tests.unit.test_orchestration_validation \
  tests.unit.test_postgres_publish \
  tests.unit.test_postgres_schema_sql
```

Run a syntax and import compile check:

```bash
PYTHONPATH=src python -m compileall -q src tests main.py
```

## Documentation

Use the README for the project-level view. Use the docs for implementation details:

| Document | Covers |
|---|---|
| [docs/01_data_generator.md](docs/01_data_generator.md) | Offline generator behavior, configuration, output layout, and Bronze ingestion contract |
| [docs/02_streaming_generator.md](docs/02_streaming_generator.md) | Streaming generator, Kafka replay, event shape, streaming problems, and Flink contract |
| [docs/03_bronze_ingestion.md](docs/03_bronze_ingestion.md) | Bronze transaction schema, metadata fields, partitioning, and raw-preservation rules |
| [docs/04_silver_transactions.md](docs/04_silver_transactions.md) | Silver transaction schema, cleaned types, standardization, deduplication, and quality rules |
| [docs/05_gold_tables.md](docs/05_gold_tables.md) | Gold fact, dimension, OBT, feature, and PostgreSQL serving schema design |
| [docs/06_feature_engineering.md](docs/06_feature_engineering.md) | Fraud feature definitions, event-time windows, point-in-time joins, leakage rules, and validation expectations |
| [docs/07_flink_streaming_pipeline.md](docs/07_flink_streaming_pipeline.md) | Flink Kafka topology, watermarks, deduplication, streaming features, alerts, late events, state, and recovery |
| [docs/08_flink_window_processing.md](docs/08_flink_window_processing.md) | Keyed five-minute Flink windows and late-event side outputs |
| [docs/09_orchestration_flow.md](docs/09_orchestration_flow.md) | Airflow DAGs, ingest/validate stages, asset dependencies, shared configuration, and local startup |
| [docs/10_airflow_workflow_demonstration.md](docs/10_airflow_workflow_demonstration.md) | Airflow UI evidence for the successful Raw, Bronze, Silver, Gold, and offline-feature workflow |
| [docs/optimization/flink/streaming_job_optimization.md](docs/optimization/flink/streaming_job_optimization.md) | Controlled Flink UI benchmark for operator chaining, parallelism, backpressure, throughput, and checkpoints |
| [docs/optimization/spark/silver_job_optimization.md](docs/optimization/spark/silver_job_optimization.md) | Spark UI baseline, Silver bottleneck analysis, AQE and shuffle-partition optimization, measured tradeoffs, and evidence |

## Engineering Direction

The offline path runs from raw CSV through Bronze, Silver, and Gold Parquet. The streaming path replays generated events through Kafka and uses Flink for validation, deduplication, event-time features, late-event handling, and fraud alerts.

The long-term platform direction is an end-to-end fraud data system with orchestration, lineage, feature generation, model tracking, scoring, and monitoring built around the generated transaction data.
