# FraudStream: the Data Platform

A card issuer needs clean, trustworthy payment data before it can score fraud. This part
turns raw payments into it: two years of offline CSVs in batches, and a live event stream,
both cleaned into Iceberg and PostgreSQL, run by Airflow and catalogued in DataHub.

![FraudStream data and ML platform](../../images/architecture/data-and-ml-platform.svg)

This part is the top half of the drawing:

| Flow | What moves |
|---|---|
| **B** batch | the offline CSVs from lakeFS's raw repository through Spark's Bronze, Silver and Gold, into Iceberg and PostgreSQL |
| **S** streaming | live events through Kafka and Flink: clean transactions, 5-minute features and late events, each to its own topic |
| **O** orchestration | Airflow starts the Spark jobs; no data passes through it |
| **G** governance | schemas, lineage and checks into DataHub |
| **I** inspection | an engineer's SQL on PostgreSQL and Trino |

## Contents

| # | Doc | The main idea |
|---|---|---|
| 01 | [Docker](01_docker.md) | one slim batch image, 13.2 GB → 1.55 GB |
| 02 | [Data generator](02_data_generator.md) | 663 million rows and a Kafka event log, with skew, duplicates and late data planted on purpose |
| 03 | [Processing jobs](03_processing_jobs.md) | Spark Bronze → Silver → Gold, Flink 5-minute windows; each planted problem fixed and measured |
| 04 | [Data storage](04_data_storage.md) | every row in Iceberg, a recent window in PostgreSQL, each tuned and measured |
| 05 | [Orchestration](05_data_pipeline_orchestration.md) | three Airflow DAGs, each started only by the last one's validated data |
| 06 | [Data governance](06_data_governance.md) | DataHub catalog, lineage and checks, filled from the repository |
| 07 | [lakeFS](07_lakefs.md) | the raw CSVs get commits and branches; a run merges only after its checks |
| 08 | [Testcontainers](08_testcontainers.md) | the real storage code tested against a throwaway lakeFS in about a minute |

## Where things are

| What | Where |
|---|---|
| The ML and LLM platform | [`docs/edai-2/`](../edai-2/) |
| The architecture drawings | [`images/architecture/`](../../images/architecture/) |
| The project overview | [`README.md`](../../README.md) |
