# Streaming Data Generator

The real-time half of the project. Spark handles the offline CSV-to-Iceberg path;
this side is Kafka and Flink:

```text
generate events -> replay them to Kafka -> Flink computes features
```

The generator writes a reproducible event log, one transaction per line in publish
order. A replay producer publishes that log to Kafka, and Flink deals with late,
out-of-order, duplicate and bursty events:

```text
data/raw_stream/transactions/topic=financial_transactions/events.jsonl
```

## Generate the log

```bash
PYTHONPATH=src python -m fraudstream.generators.streaming_transactions
```

Settings are in `configs/generator/streaming_transactions.json`. The default profile:

| Metric | Value |
|---|---:|
| Base events | 500,000 (512,500 with duplicate replay) |
| Customers / merchants | 220,000 / 45,000 |
| Simulated partitions | 24 |
| Event-time span | 7 days |
| Output size | ~487 MB |

It also writes `_manifest.json`, `_stream_summary.csv` and `_stream_summary.json`
next to the log.

## Replay to Kafka

```bash
uv sync --extra kafka
docker compose up -d kafka kafka-topic-init kafka-ui   # broker on :9092, Kafka UI on :18080
PYTHONPATH=src python -m fraudstream.producers.stream_replay \
  --bootstrap-servers localhost:9092 --topic financial_transactions --events-per-second 5000
```

| Option | Meaning |
|---|---|
| `--max-events 10000` | Replay a small sample |
| `--start-offset 100000` | Skip records first, for resume tests |
| `--events-per-second 5000` | Fixed rate; `0` means no throttling |
| `--time-mode produced_at` | Replay using the gaps between source `produced_at` times |
| `--speed-factor 3600` | Compress time: one source hour becomes one second |

`Connection refused` on `localhost:9092` means the broker isn't running. It is not a
Flink problem. Set `KAFKA_UI_PORT` if `18080` is taken, and `docker compose down`
to stop.

## Event shape

The Kafka message value is the whole JSON envelope; the key is `partition_key` (the
customer id).

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

Two times matter. `value.event_timestamp` is when the transaction happened, and Flink
windows on it. `produced_at` / `value.created_ts` is when it was published, and
lateness is measured against it. Here the event arrived about five hours after it
happened, hence `late`.

## Problems it injects

| Problem | How it appears | Flink's answer |
|---|---|---|
| Late events | `event_timestamp` far before `produced_at` | Watermarks and a late-event policy |
| Out-of-order | Event time steps backward in publish order | Process by event time |
| Duplicates | Same `event_id` appears again | Deduplicate on `event_id` |
| Bursts | Many events land in a few five-minute windows | Test window pressure |
| Windows | Every event carries its expected window | Check Flink's results |

| Setting | Meaning |
|---|---|
| `n_events`, `n_customers`, `n_merchants`, `n_partitions` | Size and cardinality |
| `late_event_rate`, `out_of_order_rate`, `duplicate_rate` | Share of each problem |
| `burst_window_count`, `burst_event_ratio` | How concentrated the bursts are |
| `window_minutes` | Event-time window size |

`late_event_rate + out_of_order_rate` must not exceed `1`: each event picks one
timing category.

## What Flink expects

Flink reads `financial_transactions`, keys by customer, uses `value.event_timestamp`
as event time, takes its watermark delay from the measured p95 first-arrival latency,
deduplicates on `value.event_id`, and computes five-minute customer and merchant
features plus velocity and burst alerts. `headers.problem_flags` is kept as evidence
only; late-event decisions come from the watermarks. The full contract is in
[07_flink_streaming_pipeline.md](07_flink_streaming_pipeline.md).

## Test it

```bash
PYTHONPATH=src python -m unittest tests.unit.test_streaming_transactions tests.unit.test_stream_replay
PYTHONPATH=src python -m compileall -q src tests main.py
```
