  # Change Data Capture: Debezium on `bronze.raw_transactions`

Debezium captures row-level changes on one PostgreSQL table --
`bronze.raw_transactions` -- and streams them to Kafka as they happen, so a
consumer can react the moment Bronze writes land instead of polling Postgres
or waiting for the next Airflow run.

## Why this one table, not every serving table

CDC is scoped to a single table on purpose:

- **It's the earliest point "new data has durably landed" is true.** Bronze
  is the first layer raw source records reach PostgreSQL; capturing changes
  there is the earliest a downstream consumer could possibly react.
- **It's structurally stable.** Bronze's write path
  (`truncate_tables_cascade` + `write_jdbc_table(..., mode="append")` in
  `fraudstream.jobs.warehouse`) always `TRUNCATE`s then re-`INSERT`s -- it
  never drops or recreates the table. A Debezium publication is tied to the
  table's existence, so a table that's periodically dropped would silently
  stop being captured; `TRUNCATE` is a captured, well-defined replication
  event instead.
- **Its real (non-local-dev) write mode is `append`.** `--write-mode
  overwrite` is a local-regeneration convenience (see `docs/03`); with
  `append`, CDC only emits events for genuinely new rows, which is the
  normal CDC shape. Every table in this project could technically be
  wired up the same way -- this just demonstrates the pattern end to end
  on the one table where it's most meaningful, rather than instrumenting
  all fifteen-plus Gold tables for a local/demo project.

Silver and Gold aren't included here because they're **derived** from Bronze
by the existing Spark jobs -- they don't need a second, independent
change-notification path; Bronze's CDC stream is upstream of everything they
compute.

## Architecture

```text
Spark (Bronze ingest) --JDBC--> PostgreSQL bronze.raw_transactions
                                      | (logical replication, pgoutput)
                                      v
                              Debezium (Kafka Connect)
                                      |
                                      v
                    Kafka topic: fraudstream.bronze.raw_transactions
```

Debezium runs as a Kafka Connect source connector (`quay.io/debezium/connect`,
which bundles Kafka Connect and the Debezium PostgreSQL connector plugin --
no separate driver or plugin install needed). It reads PostgreSQL's write-ahead
log through the `pgoutput` logical-decoding plugin, which is built into
PostgreSQL itself (available since v10) -- no Postgres extension is
installed for this.

No new database, no new storage system: Debezium doesn't store data itself,
it turns WAL changes into Kafka messages using the *same* Kafka cluster this
project already runs for the streaming pipeline.

## What changed to enable this

- **`docker-compose.yml`**: `postgres` now starts with `-c
  wal_level=logical` (required for any logical-replication consumer, not
  Postgres-specific to Debezium). This only takes effect on a container
  restart, not a data-losing reinitialize.
- **`docker-compose.yml`**: two new services --
  - `debezium-connect`: the Kafka Connect worker running Debezium's Postgres
    connector, REST API on `localhost:18083` (continuing the `1808x`
    convention already used by `kafka-ui`/`airflow`/`trino`).
  - `debezium-connector-init`: a one-shot container, matching the existing
    `kafka-topic-init`/`minio-bucket-init`/`postgres-schema-init` pattern in
    this project, that registers the connector via Kafka Connect's REST API
    once it's healthy.
- **`infra/debezium/bronze-raw-transactions-connector.json`**: the connector
  config -- what table to watch, the Kafka topic naming prefix, and the
  replication slot/publication names Debezium creates automatically.

No PostgreSQL privilege changes were needed: the `fraudstream` user is
created as a superuser by the official `postgres` image's bootstrap process,
which already includes the `REPLICATION` right needed to create a
replication slot and publication.

## Running it

```bash
docker compose up -d postgres  # restart so wal_level=logical takes effect
docker compose up -d kafka kafka-topic-init debezium-connect debezium-connector-init
```

Confirm the connector is running:

```bash
curl -s http://localhost:18083/connectors/fraudstream-bronze-raw-transactions/status | python3 -m json.tool
```

`"state": "RUNNING"` for both the connector and its task means it's caught
up and streaming.

## What you'll see

On first start, Debezium takes an **initial snapshot** -- one `"op": "r"`
(read) event per existing row in `bronze.raw_transactions` -- then switches
to streaming, emitting one event per row for every future Bronze run's
`TRUNCATE` + append. Each event's `payload.op` is one of:

| `op` | Meaning |
|---|---|
| `r` | Read, from the initial snapshot |
| `c` | Create (`INSERT`) |
| `u` | Update |
| `d` | Delete |

`bronze.raw_transactions` is insert-only in practice (Bronze never updates
or deletes existing rows), so in steady state you'll only see `r` (once)
and `c` events, plus a `TRUNCATE` reflected as a burst of deletes-then-inserts
whenever a job runs with `--write-mode overwrite`.

Inspect events from the command line:

```bash
docker exec -it fraudstream-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraudstream.bronze.raw_transactions \
  --from-beginning \
  --max-messages 5
```

Or browse the topic in Kafka UI (`http://localhost:18080`), the same UI
already used for the streaming pipeline's topics.

## Implementation Map

| Area | Location |
|---|---|
| Postgres logical replication setting | `docker-compose.yml` (`postgres` service `command`) |
| Kafka Connect + Debezium worker | `docker-compose.yml` (`debezium-connect` service) |
| Connector registration (one-shot) | `docker-compose.yml` (`debezium-connector-init` service) |
| Connector configuration | `infra/debezium/bronze-raw-transactions-connector.json` |
