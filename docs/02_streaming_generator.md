# Streaming Data Generator

This document explains the streaming side of FraudStream. The goal is simple:

```text
generate realistic transaction events -> replay them to Kafka -> process them with Flink
```

Spark is for the offline CSV/Parquet path. Flink is for the real-time streaming path.

## Core Concept

The streaming generator creates a reproducible Kafka-like event log:

```text
data/raw_stream/transactions/topic=financial_transactions/events.jsonl
```

Each line is one transaction event in publish order. The replay producer reads this file and publishes the same records to Kafka. Later, the Flink job will consume the Kafka topic and handle real streaming problems such as late events, out-of-order events, duplicates, and burst traffic.

This design keeps the stream realistic and debuggable:

- `events.jsonl` is the reproducible source log.
- Kafka is the real streaming transport.
- Flink is the future streaming processor.

## Generate The Stream Log

Run from the repository root:

```bash
PYTHONPATH=src python -m fraudstream.generators.streaming_transactions
```

Default config:

```text
configs/generator/streaming_transactions.json
```

Current default profile:

| Metric | Value |
|---|---:|
| Base events | 500,000 |
| Records after duplicate replay | 512,500 |
| Customers | 220,000 |
| Merchants | 45,000 |
| Simulated partitions | 24 |
| Event-time span | 7 days |
| Output size | ~487 MB |

Generated artifacts:

```text
data/raw_stream/transactions/
|-- _manifest.json
|-- _stream_summary.csv
|-- _stream_summary.json
`-- topic=financial_transactions/
    `-- events.jsonl
```

## Replay Events To Kafka

Install the optional Kafka dependency with `uv`:

```bash
uv sync --extra kafka
```

Start Kafka locally with Docker:

```bash
docker compose up -d kafka kafka-topic-init kafka-ui
```

This starts a single local Kafka broker on `localhost:9092`, creates the `financial_transactions` topic with 24 partitions, and opens Kafka UI at:

```text
http://localhost:18080
```

If port `18080` is already in use, choose another host port:

```bash
KAFKA_UI_PORT=18081 docker compose up -d kafka kafka-topic-init kafka-ui
```

Check that Kafka is reachable:

```bash
nc -vz localhost 9092
```

Then publish generated events to Kafka:

```bash
PYTHONPATH=src python -m fraudstream.producers.stream_replay \
  --bootstrap-servers localhost:9092 \
  --topic financial_transactions \
  --events-per-second 5000
```

Useful replay options:

| Option | Meaning |
|---|---|
| `--max-events 10000` | Replay only a small sample. |
| `--start-offset 100000` | Skip records before replaying. Useful for resume tests. |
| `--events-per-second 5000` | Fixed publish rate. Use `0` for no throttling. |
| `--time-mode produced_at` | Replay using gaps between source `produced_at` timestamps. |
| `--speed-factor 3600` | Compress source time. One source hour becomes one second. |

If you see `Connection refused` for `localhost:9092`, Kafka is not running or not listening on that address. This is a broker connection problem, not a Flink consumer problem.

Stop the local Kafka stack when finished:

```bash
docker compose down
```

## Event Shape

Each Kafka message value is the full JSON envelope from `events.jsonl`. The Kafka message key is `partition_key`, which is the customer id.

Example event:

```json
{
  "topic": "financial_transactions",
  "partition": 10,
  "partition_key": "cust_stream_00177113",
  "source_sequence": 12,
  "produced_at": "2026-07-01T00:00:17",
  "headers": {
    "event_type": "transaction.created",
    "schema_version": "stream_v1",
    "producer": "fraudstream_streaming_generator",
    "problem_flags": ["late"]
  },
  "value": {
    "event_id": "evt_stream_000000283280",
    "transaction_id": "txn_stream_000000283280",
    "customer_id": "cust_stream_00177113",
    "merchant_id": "merch_stream_00000788",
    "merchant_category": "online_marketplace",
    "amount": "45.99",
    "currency": "USD",
    "event_timestamp": "2026-06-30T18:49:58",
    "created_ts": "2026-07-01T00:00:17",
    "event_window_start": "2026-06-30T18:45:00",
    "event_window_end": "2026-06-30T18:50:00",
    "event_window_minutes": 5
  }
}
```

Important timestamp fields:

| Field | Meaning                                                                              |
|---|--------------------------------------------------------------------------------------|
| `value.event_timestamp` | When the transaction actually happened. Flink will this for event-time windows.      |
| `produced_at` / `value.created_ts` | When the event was published or arrived. Use this for lateness and freshness checks. |

In the example, the event was published at `2026-07-01T00:00:17`, but the transaction happened at `2026-06-30T18:49:58`. That is why it is marked as `late`.

## Simulated Streaming Problems

| Problem | How It Appears | Flink Handling |
|---|---|---|
| Late events | `event_timestamp` is far before `produced_at`. | Use watermarks and late-event policy. |
| Out-of-order events | Event time moves backward compared with publish order. | Process by event time, not arrival order. |
| Duplicates | Same `event_id` / `transaction_id` appears again. | Deduplicate by event id. |
| Burst traffic | Many events land in selected five-minute windows. | Test window pressure and throughput. |
| Event-time windows | Every event includes expected window boundaries. | Validate Flink window results. |

## Key Config Settings

| Setting | Meaning |
|---|---|
| `n_events` | Base event count before duplicate replay. |
| `n_customers`, `n_merchants` | Entity cardinality for realistic keys and joins. |
| `n_partitions` | Simulated topic partition count in the generated envelope. |
| `late_event_rate` | Share of events marked late. |
| `out_of_order_rate` | Share of events intentionally backdated. |
| `duplicate_rate` | Share of events replayed as duplicates. |
| `burst_window_count`, `burst_event_ratio` | Controls burst traffic concentration. |
| `window_minutes` | Event-time window size. |

`late_event_rate + out_of_order_rate` must not exceed `1` because each event chooses one main timing category: late, out-of-order, or normal.

## Downstream Contract

The future Flink job should:

- consume Kafka topic `financial_transactions`
- use Kafka key / `partition_key` for keyed customer processing
- parse the JSON envelope
- use `value.event_timestamp` as event time
- deduplicate by `value.event_id`
- compute and validate event-time windows
- track late, duplicate, out-of-order, and burst behavior from `headers.problem_flags`

## Validate

Run the streaming generator test:

```bash
PYTHONPATH=src python -m unittest tests.unit.test_streaming_transactions
```

Run the Kafka replay producer test:

```bash
PYTHONPATH=src python -m unittest tests.unit.test_stream_replay
```

Run all current unit tests:

```bash
PYTHONPATH=src python -m unittest \
  tests.unit.test_offline_transactions \
  tests.unit.test_streaming_transactions \
  tests.unit.test_stream_replay
```

Run a syntax compile check:

```bash
PYTHONPATH=src python -m compileall -q src tests main.py
```
