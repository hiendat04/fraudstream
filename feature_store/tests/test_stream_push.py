"""Unit tests for the streaming feature push job's pure parts."""

from __future__ import annotations

import unittest
import unittest.mock
from datetime import datetime, timezone

import pandas as pd
from feast.data_source import PushMode

from fraudstream_feast.spark import CUSTOMER_PUSH_TABLE_COLUMNS, MERCHANT_PUSH_TABLE_COLUMNS
from fraudstream_feast.stream_push import (
    PushSettings,
    customer_feature_row,
    flush_batch,
    merchant_feature_row,
)


CUSTOMER_PAYLOAD = {
    "feature_id": "customer_5m:C-1:1767225600000",
    "feature_type": "customer_5m",
    "entity_type": "customer",
    "entity_id": "C-1",
    "window_start": "2026-01-01T00:00:00Z",
    "window_end": "2026-01-01T00:05:00Z",
    "window_size_minutes": 5,
    "txn_count": 3,
    "amount_sum": "310.50",
    "amount_avg": "103.50",
    "amount_max": "200.00",
    "declined_txn_count": 1,
    "last_event_timestamp": "2026-01-01T00:04:12Z",
    "is_correction": False,
    "emitted_at": "2026-01-01T00:05:30Z",
    "customer_id": "C-1",
    "distinct_merchant_count": 2,
    "distinct_device_count": 1,
}

MERCHANT_PAYLOAD = {
    **{key: value for key, value in CUSTOMER_PAYLOAD.items()
       if key not in ("customer_id", "distinct_merchant_count", "distinct_device_count")},
    "feature_type": "merchant_5m",
    "entity_type": "merchant",
    "entity_id": "M-9",
    "merchant_id": "M-9",
    "merchant_category": "online_marketplace",
    "distinct_customer_count": 3,
}


class RecordMappingTest(unittest.TestCase):
    def test_customer_row_matches_the_landing_table_columns(self):
        """A pushed dataframe must carry exactly the batch source's columns, in order."""

        self.assertEqual(tuple(customer_feature_row(CUSTOMER_PAYLOAD)), CUSTOMER_PUSH_TABLE_COLUMNS)

    def test_merchant_row_matches_the_landing_table_columns(self):
        """Same contract on the merchant side."""

        self.assertEqual(tuple(merchant_feature_row(MERCHANT_PAYLOAD)), MERCHANT_PUSH_TABLE_COLUMNS)

    def test_event_timestamp_is_the_window_close_and_created_is_the_emit_time(self):
        """A window's features are only true as of its close; `created` records when Flink emitted them."""

        row = customer_feature_row(CUSTOMER_PAYLOAD)

        self.assertEqual(row["event_timestamp"], datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc))
        self.assertEqual(row["created"], datetime(2026, 1, 1, 0, 5, 30, tzinfo=timezone.utc))

    def test_string_amounts_become_floats(self):
        """Flink emits decimal amounts as strings; the feature store needs numbers."""

        row = customer_feature_row(CUSTOMER_PAYLOAD)

        self.assertEqual(row["amount_sum"], 310.5)
        self.assertEqual(row["amount_max"], 200.0)


class RecordingStore:
    """Minimal stand-in for FeatureStore that records what push() was called with."""

    def __init__(self):
        self.calls = []

    def push(self, push_source_name, df, to=None):
        self.calls.append((push_source_name, df, to))


class FakeWriter:
    """Stand-in for the object `DataFrame.writeTo(table)` returns."""

    def __init__(self, recorder, table, rows, schema):
        self._recorder = recorder
        self.table = table
        self.rows = rows
        self.schema = schema

    def append(self):
        self._recorder.appends.append({"table": self.table, "rows": self.rows, "schema": self.schema})


class FakeDataFrame:
    """Stand-in for the object `SparkSession.createDataFrame(...)` returns."""

    def __init__(self, recorder, rows, schema):
        self._recorder = recorder
        self.rows = rows
        self.schema = schema

    def writeTo(self, table):
        return FakeWriter(self._recorder, table, self.rows, self.schema)


class FakeTable:
    """Stand-in for the object `SparkSession.table(name)` returns."""

    def __init__(self, schema):
        self.schema = schema


class StoppableRecordingSpark:
    """RecordingSpark plus the stop() the job calls on shutdown."""

    def __init__(self, schemas):
        self._inner = RecordingSpark(schemas)
        self.stopped = False

    def table(self, name):
        """Delegate to the wrapped recorder."""

        return self._inner.table(name)

    def createDataFrame(self, rows, schema=None):
        """Delegate to the wrapped recorder."""

        return self._inner.createDataFrame(rows, schema=schema)

    def stop(self):
        """Record that the session was stopped."""

        self.stopped = True


class RecordingSpark:
    """Minimal stand-in for SparkSession that records the append() and the schema lookup it is asked to do."""

    def __init__(self, table_schemas):
        self.appends = []
        self.table_calls = []
        self._table_schemas = table_schemas

    def table(self, name):
        self.table_calls.append(name)
        return FakeTable(self._table_schemas[name])

    def createDataFrame(self, data, schema=None):
        return FakeDataFrame(self, list(data), schema)


CUSTOMER_TABLE = "iceberg.feature_store.stream_customer_features_5m"
MERCHANT_TABLE = "iceberg.feature_store.stream_merchant_features_5m"


class FlushBatchTest(unittest.TestCase):
    def test_flush_pushes_online_via_feast_and_appends_offline_via_spark(self):
        """The online write goes through Feast (Redis); the offline write is a direct Spark append (Iceberg)."""

        store = RecordingStore()
        # A sentinel distinct from `columns` proves the append uses the
        # table's real schema, not one inferred from the column names.
        table_schema = object()
        spark = RecordingSpark({CUSTOMER_TABLE: table_schema})
        rows = [customer_feature_row(CUSTOMER_PAYLOAD)]

        written = flush_batch(
            store,
            "customer_features_5m_push",
            rows,
            spark=spark,
            table=CUSTOMER_TABLE,
            columns=CUSTOMER_PUSH_TABLE_COLUMNS,
        )

        self.assertEqual(written, 1)

        # Online store (Redis): exactly one Feast push, mode ONLINE only.
        self.assertEqual(len(store.calls), 1)
        push_source_name, df, mode = store.calls[0]
        self.assertEqual(push_source_name, "customer_features_5m_push")
        self.assertEqual(mode, PushMode.ONLINE)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(tuple(df.columns), CUSTOMER_PUSH_TABLE_COLUMNS)

        # Offline store (Iceberg landing table): exactly one Spark append,
        # built against the table's own schema (fetched via spark.table()).
        self.assertEqual(spark.table_calls, [CUSTOMER_TABLE])
        self.assertEqual(len(spark.appends), 1)
        append = spark.appends[0]
        self.assertEqual(append["table"], CUSTOMER_TABLE)
        self.assertIs(append["schema"], table_schema)
        self.assertEqual(append["rows"], [tuple(rows[0][column] for column in CUSTOMER_PUSH_TABLE_COLUMNS)])

    def test_flushing_nothing_does_not_call_push_or_append(self):
        """An idle interval must not create an empty Redis write or an empty Iceberg commit."""

        store = RecordingStore()
        spark = RecordingSpark({CUSTOMER_TABLE: object()})

        written = flush_batch(
            store,
            "customer_features_5m_push",
            [],
            spark=spark,
            table=CUSTOMER_TABLE,
            columns=CUSTOMER_PUSH_TABLE_COLUMNS,
        )

        self.assertEqual(written, 0)
        self.assertEqual(store.calls, [])
        self.assertEqual(spark.table_calls, [])
        self.assertEqual(spark.appends, [])


class PushSettingsTest(unittest.TestCase):
    def test_from_env_reads_the_spark_master(self):
        """FEAST_SPARK_MASTER must reach the session this job itself builds, not just Feast's unused one."""

        with unittest.mock.patch.dict("os.environ", {"FEAST_SPARK_MASTER": "local[2]"}, clear=False):
            settings = PushSettings.from_env()

        self.assertEqual(settings.spark_master, "local[2]")

    def test_from_env_defaults_the_spark_master(self):
        """Absent the env var, the job keeps the same default `build_spark_session` already had."""

        with unittest.mock.patch.dict("os.environ", {}, clear=True):
            settings = PushSettings.from_env()

        self.assertEqual(settings.spark_master, "local[4]")


if __name__ == "__main__":
    unittest.main()


class LoopStopped(Exception):
    """Raised by the fake consumer to break out of the endless poll loop."""


class RecordingConsumer:
    """Kafka consumer stand-in that polls empty and then stops the loop."""

    def __init__(self, max_polls=5):
        self._max_polls = max_polls
        self.polls = 0
        self.commits = 0
        self.closed = False
        self.subscribed = None

    def subscribe(self, topics):
        """Record the subscribed topic list."""

        self.subscribed = topics

    def poll(self, timeout):  # noqa: ARG002 - matches the confluent_kafka signature
        """Return nothing, then stop the loop once the poll budget is spent."""

        self.polls += 1
        if self.polls > self._max_polls:
            raise LoopStopped
        return None

    def commit(self, asynchronous=False):  # noqa: ARG002 - matches the real signature
        """Count commits. The real client raises _NO_OFFSET when none are stored."""

        self.commits += 1

    def close(self):
        """Record that the consumer was closed."""

        self.closed = True


class IdleLoopTest(unittest.TestCase):
    """An idle flush tick must not commit.

    librdkafka raises `_NO_OFFSET` when `commit()` is called before anything
    has been consumed. Committing on every timer tick therefore killed the
    job as soon as it drained its backlog and the stream went quiet.
    """

    def test_idle_flush_ticks_never_commit(self):
        """Five due-but-empty ticks must produce zero commits and close cleanly."""

        import fraudstream_feast.stream_push as sp

        consumer = RecordingConsumer(max_polls=5)
        spark = StoppableRecordingSpark({CUSTOMER_TABLE: object(), MERCHANT_TABLE: object()})
        tables = {sp.CUSTOMER_PUSH_SOURCE: CUSTOMER_TABLE, sp.MERCHANT_PUSH_SOURCE: MERCHANT_TABLE}

        with unittest.mock.patch("confluent_kafka.Consumer", lambda conf: consumer), \
             unittest.mock.patch.object(sp, "FeatureStore", lambda repo_path: RecordingStore()), \
             unittest.mock.patch.object(sp, "_ensure_landing_tables", lambda s, st: tables), \
             unittest.mock.patch("fraudstream_feast.spark.build_spark_session", lambda **kw: spark), \
             unittest.mock.patch("fraudstream_feast.spark.configs_from_env", lambda: (None, None, None)):
            settings = sp.PushSettings(flush_interval_seconds=0.0, poll_timeout_seconds=0.0)
            with self.assertRaises(LoopStopped):
                sp.run_stream_push(settings)

        self.assertEqual(consumer.polls, 6)
        self.assertEqual(consumer.commits, 0, "idle ticks must not commit -- librdkafka raises _NO_OFFSET")
        self.assertTrue(consumer.closed, "the consumer must still be closed via the finally block")
        self.assertTrue(spark.stopped, "the Spark session must be stopped on shutdown")
