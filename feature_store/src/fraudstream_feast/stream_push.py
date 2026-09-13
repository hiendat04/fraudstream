"""Push Flink's 5-minute window features into the Feast offline and online stores.

This is a deployment job, not a pipeline step: it runs continuously beside the
Flink job, consuming the two derived feature topics and handing each
micro-batch to both stores. Feast's Spark offline store can only write
parquet/csv/json/avro batch sources, never Iceberg (see
`fraudstream_feast.spark`), so each flush is two writes rather than one Feast
call: `store.push(..., to=PushMode.ONLINE)` for Redis, and a direct Spark
`writeTo(...).append()` for the Iceberg landing table. Both write sites live
together in `flush_batch`.

Records are batched rather than pushed individually because the offline half
of a flush is an Iceberg commit: one commit per record would produce a data
file per record and make the table unreadable in short order.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

from feast import FeatureStore
from feast.data_source import PushMode

from fraudstream_feast.spark import (
    CUSTOMER_PUSH_TABLE_COLUMNS,
    MERCHANT_PUSH_TABLE_COLUMNS,
)


LOGGER = logging.getLogger("fraudstream.feast.stream_push")

CUSTOMER_TOPIC = "fraud_features_customer_5m"
MERCHANT_TOPIC = "fraud_features_merchant_5m"
CUSTOMER_PUSH_SOURCE = "customer_features_5m_push"
MERCHANT_PUSH_SOURCE = "merchant_features_5m_push"

PUSH_SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    CUSTOMER_PUSH_SOURCE: CUSTOMER_PUSH_TABLE_COLUMNS,
    MERCHANT_PUSH_SOURCE: MERCHANT_PUSH_TABLE_COLUMNS,
}


@dataclass(frozen=True)
class PushSettings:
    """Runtime settings for the stream-push deployment job."""

    repo_path: str = "feature_store/feature_repo"
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "fraudstream-feast-stream-push"
    # Flush thresholds bound both the online write latency and the size of
    # each Iceberg append.
    batch_size: int = 10000
    flush_interval_seconds: float = 30.0
    poll_timeout_seconds: float = 1.0
    catalog_name: str = "iceberg"
    spark_master: str = "local[4]"

    @classmethod
    def from_env(cls) -> "PushSettings":
        """Build settings from the environment the containers already set."""

        return cls(
            repo_path=os.environ.get("FEAST_REPO_PATH", cls.repo_path),
            bootstrap_servers=os.environ.get(
                "FRAUDSTREAM_KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers
            ),
            batch_size=int(os.environ.get("FEAST_PUSH_BATCH_SIZE", "500")),
            flush_interval_seconds=float(os.environ.get("FEAST_PUSH_FLUSH_SECONDS", "30")),
            catalog_name=os.environ.get("FRAUDSTREAM_ICEBERG_CATALOG_NAME", "iceberg"),
            spark_master=os.environ.get("FEAST_SPARK_MASTER", cls.spark_master),
        )


def _parse_timestamp(value: str) -> datetime:
    """Parse one of Flink's UTC timestamp strings into an aware datetime."""

    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _shared_feature_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the feature fields both entity types share."""

    return {
        "event_timestamp": _parse_timestamp(payload["window_end"]),
        "created": _parse_timestamp(payload["emitted_at"]),
        "txn_count": int(payload["txn_count"]),
        "amount_sum": float(payload["amount_sum"]),
        "amount_avg": float(payload["amount_avg"]),
        "amount_max": float(payload["amount_max"]),
        "declined_txn_count": int(payload["declined_txn_count"]),
        "is_correction": bool(payload["is_correction"]),
    }


def customer_feature_row(payload: dict[str, Any]) -> dict[str, Any]:
    """Map one customer feature payload to a landing-table row."""

    row = {
        "customer_id": payload["customer_id"],
        **_shared_feature_fields(payload),
        "distinct_merchant_count": int(payload["distinct_merchant_count"]),
        "distinct_device_count": int(payload["distinct_device_count"]),
    }
    return {column: row[column] for column in CUSTOMER_PUSH_TABLE_COLUMNS}


def merchant_feature_row(payload: dict[str, Any]) -> dict[str, Any]:
    """Map one merchant feature payload to a landing-table row."""

    row = {
        "merchant_id": payload["merchant_id"],
        **_shared_feature_fields(payload),
        "distinct_customer_count": int(payload["distinct_customer_count"]),
        "merchant_category": payload.get("merchant_category"),
    }
    return {column: row[column] for column in MERCHANT_PUSH_TABLE_COLUMNS}


def flush_batch(
    store: Any,
    push_source_name: str,
    rows: list[dict[str, Any]],
    *,
    spark: Any,
    table: str,
    columns: tuple[str, ...],
) -> int:
    """Write one accumulated batch to Redis via Feast and to Iceberg via a direct Spark append.

    Feast's Spark offline store can only write parquet/csv/json/avro batch
    sources, so it cannot commit to an Iceberg table (see
    `fraudstream_feast.spark`). The online half of the push therefore still
    goes through Feast; the offline half is a plain Spark append against the
    same landing table Feast reads from for historical retrieval.
    """

    if not rows:
        return 0

    import pandas as pd

    frame = pd.DataFrame(rows, columns=list(columns))

    # Online store (Redis): written through Feast's push path.
    store.push(push_source_name, frame, to=PushMode.ONLINE)

    # Offline store (Iceberg landing table): written directly with Spark,
    # since Feast cannot write Iceberg itself. The DataFrame is built against
    # the table's own schema rather than one inferred from these Python
    # values -- an all-None column (e.g. a merchant batch with no
    # merchant_category set) would otherwise infer as NullType, which
    # Iceberg cannot bind to the table's declared STRING column.
    ordered_rows = [tuple(row[column] for column in columns) for row in rows]
    table_schema = spark.table(table).schema
    spark.createDataFrame(ordered_rows, schema=table_schema).writeTo(table).append()

    return len(rows)


TOPIC_HANDLERS: dict[str, tuple[str, Callable[[dict[str, Any]], dict[str, Any]]]] = {
    CUSTOMER_TOPIC: (CUSTOMER_PUSH_SOURCE, customer_feature_row),
    MERCHANT_TOPIC: (MERCHANT_PUSH_SOURCE, merchant_feature_row),
}


def _ensure_landing_tables(spark: Any, settings: PushSettings) -> dict[str, str]:
    """Create the Iceberg landing tables before the first append.

    Our own Spark append targets these tables on every flush, so they must
    exist before the first batch is written rather than being created
    lazily by the write itself.
    """

    from fraudstream_feast.spark import create_push_target_tables

    customer_table, merchant_table = create_push_target_tables(spark, catalog_name=settings.catalog_name)
    return {
        CUSTOMER_PUSH_SOURCE: customer_table,
        MERCHANT_PUSH_SOURCE: merchant_table,
    }


def run_stream_push(settings: PushSettings, *, max_batches: int | None = None) -> dict[str, int]:
    """Consume the Flink feature topics and push each micro-batch to Feast and Spark."""

    from confluent_kafka import Consumer

    from fraudstream_feast.spark import build_spark_session, configs_from_env

    # One Spark session for the whole job: built once here for both the
    # landing-table bootstrap and every batch append, stopped only on
    # shutdown. A session per batch would be catastrophically slow.
    warehouse, iceberg, postgres = configs_from_env()
    spark = build_spark_session(
        warehouse=warehouse,
        iceberg=iceberg,
        postgres=postgres,
        app_name="FraudStreamFeastStreamPush",
        master=settings.spark_master,
    )
    try:
        push_tables = _ensure_landing_tables(spark, settings)
        store = FeatureStore(repo_path=settings.repo_path)
        consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group,
                "auto.offset.reset": "earliest",
                # Offsets are committed only after a successful flush, so a
                # crash replays the batch instead of losing it.
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(list(TOPIC_HANDLERS))

        pending: dict[str, list[dict[str, Any]]] = {name: [] for name, _ in TOPIC_HANDLERS.values()}
        pushed: dict[str, int] = {name: 0 for name in pending}
        running = True
        batches = 0

        def _stop(signum, frame):  # noqa: ARG001 - signal handler signature
            # Without this, SIGTERM from `docker compose down` skips the
            # finally block below and drops whatever is buffered.
            nonlocal running
            running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        def _flush_pending() -> int:
            """Flush every pending push source's buffered rows, returning the row count."""

            flushed = 0
            for push_source_name, rows in pending.items():
                written = flush_batch(
                    store,
                    push_source_name,
                    rows,
                    spark=spark,
                    table=push_tables[push_source_name],
                    columns=PUSH_SOURCE_COLUMNS[push_source_name],
                )
                pushed[push_source_name] += written
                flushed += written
                rows.clear()
            return flushed

        last_flush = time.monotonic()
        # Kafka stores no offset until a message is consumed, and committing
        # with none stored raises _NO_OFFSET. An idle flush tick -- the timer
        # firing while the stream is quiet -- would otherwise kill the job the
        # moment it catches up, so commits are gated on having consumed
        # something since the last one.
        consumed_since_commit = False
        try:
            while running:
                message = consumer.poll(settings.poll_timeout_seconds)
                if message is not None and message.error() is None:
                    consumed_since_commit = True
                    push_source_name, mapper = TOPIC_HANDLERS[message.topic()]
                    try:
                        pending[push_source_name].append(mapper(json.loads(message.value())))
                    except (KeyError, ValueError):
                        LOGGER.exception("Skipping unparseable feature record on %s", message.topic())
                elif message is not None:
                    LOGGER.error("Kafka error: %s", message.error())

                due = time.monotonic() - last_flush >= settings.flush_interval_seconds
                full = any(len(rows) >= settings.batch_size for rows in pending.values())
                if not (due or full):
                    continue

                # Delivery is at-least-once: the landing tables are
                # append-only, so a failure partway through _flush_pending()
                # (one push source written, the next raising) leaves the
                # offsets uncommitted and replays the whole batch on
                # restart, re-appending the rows already written.
                flushed = _flush_pending()
                if consumed_since_commit:
                    consumer.commit(asynchronous=False)
                    consumed_since_commit = False
                last_flush = time.monotonic()
                if flushed:
                    batches += 1
                    LOGGER.info("Flushed batch %s; totals so far: %s", batches, pushed)
                    if max_batches is not None and batches >= max_batches:
                        running = False
        finally:
            _flush_pending()
            if consumed_since_commit:
                consumer.commit(asynchronous=False)
            consumer.close()
    finally:
        spark.stop()

    return pushed


def main(argv: list[str] | None = None) -> int:
    """Run the stream-push deployment job from the command line."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=None, help="Feast repo directory. Defaults to $FEAST_REPO_PATH.")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after this many flushes (smoke tests).")
    args = parser.parse_args(argv)

    settings = PushSettings.from_env()
    if args.repo_path:
        settings = replace(settings, repo_path=args.repo_path)
    totals = run_stream_push(settings, max_batches=args.max_batches)
    json.dump(totals, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
