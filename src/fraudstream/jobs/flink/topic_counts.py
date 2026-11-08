"""Count the Flink job's output topics and check they balance against the replayed source.

Every source record ends up in exactly one of the clean, duplicate and invalid
topics, so their sum must equal the source. Late events and corrections are
counted alongside. Only committed records are read, the same ones a downstream
consumer sees.

Run from the repo root, with Kafka up, after the Flink job has caught up:

    PYTHONPATH=src uv run --extra kafka python -m fraudstream.jobs.flink.topic_counts
    PYTHONPATH=src uv run --extra kafka python -m fraudstream.jobs.flink.topic_counts --suffix _baseline
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from fraudstream.jobs.flink.transactions import (
    DEFAULT_ALERT_TOPIC,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CLEAN_TOPIC,
    DEFAULT_CUSTOMER_FEATURE_TOPIC,
    DEFAULT_DUPLICATE_TOPIC,
    DEFAULT_INVALID_TOPIC,
    DEFAULT_LATE_TOPIC,
    DEFAULT_MERCHANT_FEATURE_TOPIC,
)
from fraudstream.producers.stream_replay import DEFAULT_SOURCE_PATH

DEFAULT_OUTPUT_DIR = Path("data/flink")
TOPICS_BY_ROLE = {
    "clean": DEFAULT_CLEAN_TOPIC,
    "duplicate": DEFAULT_DUPLICATE_TOPIC,
    "invalid": DEFAULT_INVALID_TOPIC,
    "late": DEFAULT_LATE_TOPIC,
    "customer_features": DEFAULT_CUSTOMER_FEATURE_TOPIC,
    "merchant_features": DEFAULT_MERCHANT_FEATURE_TOPIC,
    "alerts": DEFAULT_ALERT_TOPIC,
}
CONSUMER_SETTINGS = {
    # Flink writes with Kafka transactions; aborted records must not be counted.
    "isolation.level": "read_committed",
    "enable.partition.eof": True,
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
}
IDLE_SECONDS = 30.0


def count_committed(consumer: Any, topics: Sequence[str]) -> dict[str, int]:
    """Read every topic to its end and return how many committed records each holds."""

    counts = {topic: 0 for topic in topics}

    def count(message: Any) -> None:
        counts[message.topic()] += 1

    _read_to_end(consumer, topics, count)
    return counts


def count_corrections(consumer: Any, feature_topics: Sequence[str]) -> int:
    """How many feature rows replace a window that had already been emitted."""

    corrections = 0

    def count(message: Any) -> None:
        nonlocal corrections
        corrections += bool(json.loads(message.value()).get("is_correction"))

    _read_to_end(consumer, feature_topics, count)
    return corrections


def reconcile(counts: dict[str, int], source_records: int) -> dict[str, Any]:
    """Check that the source equals clean + duplicate + invalid."""

    accounted = counts["clean"] + counts["duplicate"] + counts["invalid"]
    return {
        "source_records": source_records,
        "clean": counts["clean"],
        "duplicate": counts["duplicate"],
        "invalid": counts["invalid"],
        "accounted": accounted,
        "gap": source_records - accounted,
        "balanced": accounted == source_records,
    }


def _read_to_end(consumer: Any, topics: Sequence[str], on_message: Callable[[Any], None]) -> None:
    """Hand each record to `on_message` until every partition of every topic reports its end."""

    from confluent_kafka import KafkaError

    pending: set[tuple[str, int]] = set()
    for topic in topics:
        metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        if metadata.error is not None or not metadata.partitions:
            raise RuntimeError(f"Kafka topic {topic} does not exist: {metadata.error}")
        pending.update((topic, partition) for partition in metadata.partitions)

    consumer.subscribe(list(topics))
    last_record_at = time.monotonic()
    while pending:
        message = consumer.poll(1.0)
        if message is None:
            if time.monotonic() - last_record_at > IDLE_SECONDS:
                raise TimeoutError(f"no records for {IDLE_SECONDS:.0f} s; still waiting on {sorted(pending)}")
            continue
        last_record_at = time.monotonic()
        error = message.error()
        if error is not None:
            if error.code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(f"Kafka read failed: {error}")
            pending.discard((message.topic(), message.partition()))
            continue
        on_message(message)
    consumer.unsubscribe()


def _new_consumer(bootstrap_servers: str) -> Any:
    from confluent_kafka import Consumer

    # A fresh group each time, so every read starts at the beginning of each topic.
    return Consumer(
        {**CONSUMER_SETTINGS, "bootstrap.servers": bootstrap_servers, "group.id": f"fraudstream-topic-counts-{uuid.uuid4()}"}
    )


def _count_lines(path: Path) -> int:
    with path.open("rb") as source:
        return sum(1 for _ in source)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Count the Flink output topics and check they balance against the source.")
    parser.add_argument("--suffix", default="", help='Added to every output topic name, for example "_baseline".')
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE_PATH, help="The replayed JSONL log.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Write data/flink/topic_counts<suffix>.json."""

    args = build_parser().parse_args(argv)
    topics = {role: f"{topic}{args.suffix}" for role, topic in TOPICS_BY_ROLE.items()}
    by_topic = count_committed(_new_consumer(args.bootstrap_servers), list(topics.values()))
    counts = {role: by_topic[topic] for role, topic in topics.items()}
    corrections = count_corrections(
        _new_consumer(args.bootstrap_servers), [topics["customer_features"], topics["merchant_features"]]
    )
    result = {
        "suffix": args.suffix,
        "topics": topics,
        "counts": counts,
        "feature_corrections": corrections,
        "reconciliation": reconcile(counts, _count_lines(args.source)),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / f"topic_counts{args.suffix}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["reconciliation"]["balanced"] else 1


if __name__ == "__main__":
    sys.exit(main())
