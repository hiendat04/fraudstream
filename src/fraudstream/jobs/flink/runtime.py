"""PyFlink runtime topology for FraudStream streaming features.

This module is imported only by the Python 3.12 Flink environment. Pure event
validation and feature functions remain in ``transactions.py`` so the main
Python 3.14 project can test them without importing PyFlink.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from pyflink.common import Configuration, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    FlatMapFunction,
    KeyedProcessFunction,
    ProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import Time, TumblingEventTimeWindows

from fraudstream.jobs.flink.transactions import (
    StreamingFeatureConfig,
    add_event_to_accumulator,
    build_alert_records,
    build_feature_record,
    build_invalid_record,
    build_late_record,
    create_feature_accumulator,
    event_business_fingerprint,
    extract_event_timestamp_ms,
    json_dumps,
    merge_feature_accumulators,
    parse_transaction_message,
)


INVALID_OUTPUT = OutputTag("invalid-transactions", Types.STRING())
DUPLICATE_OUTPUT = OutputTag("duplicate-transactions", Types.STRING())
CUSTOMER_LATE_OUTPUT = OutputTag("late-customer-window", Types.STRING())
MERCHANT_LATE_OUTPUT = OutputTag("late-merchant-window", Types.STRING())


class EnvelopeTimestampAssigner(TimestampAssigner):
    """Assign event time directly from the raw Kafka JSON envelope."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        del record_timestamp
        return extract_event_timestamp_ms(value)


class ParseTransactionFunction(ProcessFunction):
    """Normalize valid transactions and side-output invalid source records."""

    def process_element(self, value: str, ctx: ProcessFunction.Context) -> Iterable[str]:
        del ctx
        result = parse_transaction_message(value)
        if result.is_valid:
            assert result.event is not None
            yield json_dumps(result.event)
            return
        yield INVALID_OUTPUT, json_dumps(build_invalid_record(result))


class DeduplicateTransactionFunction(KeyedProcessFunction):
    """Keep the first event ID and audit identical or conflicting replays."""

    def __init__(self, ttl_hours: int) -> None:
        self._ttl_milliseconds = ttl_hours * 60 * 60 * 1_000
        self._first_seen_state: Any = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._first_seen_state = runtime_context.get_state(
            ValueStateDescriptor("first-seen-event", Types.STRING())
        )

    def process_element(
        self,
        value: str,
        ctx: KeyedProcessFunction.Context,
    ) -> Iterable[str]:
        event = json.loads(value)
        fingerprint = event_business_fingerprint(event)
        current_processing_time = ctx.timer_service().current_processing_time()
        state_value = self._first_seen_state.value()
        first_seen = json.loads(state_value) if state_value else None

        if first_seen and current_processing_time < first_seen["expires_at_ms"]:
            duplicate_record = {
                "event_id": event["event_id"],
                "transaction_id": event["transaction_id"],
                "duplicate_type": (
                    "identical_replay"
                    if fingerprint == first_seen["business_fingerprint"]
                    else "duplicate_payload_conflict"
                ),
                "payload_matches": fingerprint == first_seen["business_fingerprint"],
                "first_source_partition": first_seen["source_partition"],
                "first_source_sequence": first_seen["source_sequence"],
                "duplicate_source_partition": event["source_partition"],
                "duplicate_source_sequence": event["source_sequence"],
                "detected_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            }
            yield DUPLICATE_OUTPUT, json_dumps(duplicate_record)
            return

        expires_at_ms = current_processing_time + self._ttl_milliseconds
        self._first_seen_state.update(
            json_dumps(
                {
                    "business_fingerprint": fingerprint,
                    "source_partition": event["source_partition"],
                    "source_sequence": event["source_sequence"],
                    "expires_at_ms": expires_at_ms,
                }
            )
        )
        ctx.timer_service().register_processing_time_timer(expires_at_ms)
        yield value

    def on_timer(
        self,
        timestamp: int,
        ctx: KeyedProcessFunction.OnTimerContext,
    ) -> Iterable[str]:
        del ctx
        state_value = self._first_seen_state.value()
        if state_value and timestamp >= json.loads(state_value)["expires_at_ms"]:
            self._first_seen_state.clear()
        return ()


class FeatureAggregateFunction(AggregateFunction):
    """Incrementally aggregate one five-minute customer or merchant window."""

    def __init__(self, entity_type: str) -> None:
        self._entity_type = entity_type

    def create_accumulator(self) -> dict[str, Any]:
        return create_feature_accumulator(self._entity_type)

    def add(self, value: str, accumulator: dict[str, Any]) -> dict[str, Any]:
        return add_event_to_accumulator(accumulator, json.loads(value))

    def get_result(self, accumulator: dict[str, Any]) -> dict[str, Any]:
        return accumulator

    def merge(self, left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
        return merge_feature_accumulators(left, right)


class FeatureWindowFunction(ProcessWindowFunction):
    """Attach entity and window identity to one incremental aggregate."""

    def __init__(self, entity_type: str) -> None:
        self._entity_type = entity_type
        self._emitted_descriptor = ValueStateDescriptor(
            f"{entity_type}-window-emitted",
            Types.BOOLEAN(),
        )

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[dict[str, Any]],
    ) -> Iterable[str]:
        emitted_state = context.window_state().get_state(self._emitted_descriptor)
        is_correction = bool(emitted_state.value())
        emitted_state.update(True)
        accumulator = next(iter(elements))
        yield json_dumps(
            build_feature_record(
                entity_type=self._entity_type,
                entity_id=key,
                window_start_ms=context.window().start,
                window_end_ms=context.window().end,
                accumulator=accumulator,
                is_correction=is_correction,
            )
        )

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        context.window_state().get_state(self._emitted_descriptor).clear()


class FeatureAlertFunction(FlatMapFunction):
    """Emit zero or more deterministic alerts from one window feature."""

    def __init__(self, config: StreamingFeatureConfig) -> None:
        self._config = config

    def flat_map(self, value: str) -> Iterable[str]:
        feature = json.loads(value)
        for alert in build_alert_records(feature, self._config):
            yield json_dumps(alert)


class LateRecordFunction:
    """Convert a window side output into the shared late-event contract."""

    def __init__(self, window_name: str, window_minutes: int) -> None:
        self._window_name = window_name
        self._window_minutes = window_minutes

    def __call__(self, value: str) -> str:
        return json_dumps(
            build_late_record(
                json.loads(value),
                window_name=self._window_name,
                window_minutes=self._window_minutes,
            )
        )


def execute_streaming_feature_job(config: StreamingFeatureConfig) -> None:
    """Execute the Kafka-to-Kafka Flink topology."""

    config = config.apply_benchmark_profile().resolve_watermark_delay()
    environment = build_streaming_feature_topology(config)
    environment.execute(config.job_name)


def build_streaming_feature_topology(config: StreamingFeatureConfig) -> StreamExecutionEnvironment:
    """Build the Flink topology without submitting its unbounded job."""

    config = config.apply_benchmark_profile().resolve_watermark_delay()
    config.validate(require_connector_jar=True)
    assert config.watermark_delay_seconds is not None
    environment = _build_environment(config)
    source = _build_source(config)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(config.watermark_delay_seconds)
        )
        .with_idleness(Duration.of_seconds(config.idle_partition_timeout_seconds))
        .with_timestamp_assigner(EnvelopeTimestampAssigner())
    )
    source_events = (
        environment.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "Kafka transaction source",
        )
        .name(f"Kafka: {config.source_topic}")
        .uid("fraudstream-kafka-transaction-source-v1")
    )
    raw_events = (
        source_events.assign_timestamps_and_watermarks(watermark_strategy)
        .name("Assign transaction event time and watermarks")
        .uid("fraudstream-transaction-watermarks-v1")
    )

    parsed_events = (
        raw_events.process(ParseTransactionFunction(), output_type=Types.STRING())
        .name("Validate and normalize transaction envelopes")
        .uid("fraudstream-parse-transactions-v1")
    )
    invalid_events = parsed_events.get_side_output(INVALID_OUTPUT)
    deduplicated_events = (
        parsed_events.key_by(_event_id, key_type=Types.STRING())
        .process(
            DeduplicateTransactionFunction(config.deduplication_ttl_hours),
            output_type=Types.STRING(),
        )
        .name("Deduplicate transaction event IDs")
        .uid("fraudstream-deduplicate-event-id-v1")
    )
    duplicate_events = deduplicated_events.get_side_output(DUPLICATE_OUTPUT)

    customer_features, customer_late = _build_feature_window(
        deduplicated_events,
        entity_type="customer",
        key_selector=_customer_id,
        late_output=CUSTOMER_LATE_OUTPUT,
        config=config,
    )
    merchant_features, merchant_late = _build_feature_window(
        deduplicated_events,
        entity_type="merchant",
        key_selector=_merchant_id,
        late_output=MERCHANT_LATE_OUTPUT,
        config=config,
    )
    late_events = customer_late.union(merchant_late)
    alerts = (
        customer_features.union(merchant_features)
        .flat_map(FeatureAlertFunction(config), output_type=Types.STRING())
        .name("Evaluate streaming fraud alert rules")
        .uid("fraudstream-feature-alert-rules-v1")
    )

    _sink(deduplicated_events, config.clean_topic, "clean-transactions", config)
    _sink(invalid_events, config.invalid_topic, "invalid-transactions", config)
    _sink(duplicate_events, config.duplicate_topic, "duplicate-transactions", config)
    _sink(late_events, config.late_topic, "late-transactions", config)
    _sink(customer_features, config.customer_feature_topic, "customer-features", config)
    _sink(merchant_features, config.merchant_feature_topic, "merchant-features", config)
    _sink(alerts, config.alert_topic, "fraud-alerts", config)

    return environment


def _build_environment(config: StreamingFeatureConfig) -> StreamExecutionEnvironment:
    runtime_settings = Configuration()
    if config.flink_ui_enabled:
        runtime_settings.set_string("rest.address", "localhost")
        runtime_settings.set_integer("rest.port", config.flink_ui_port)
        runtime_settings.set_string("rest.bind-port", str(config.flink_ui_port))
        runtime_settings.set_boolean("local.start-webserver", True)

    environment = StreamExecutionEnvironment.get_execution_environment(runtime_settings)
    environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    environment.set_parallelism(config.parallelism)
    if not config.operator_chaining_enabled:
        environment.disable_operator_chaining()
    environment.add_jars(_jar_uri(config.connector_jar))
    environment.get_config().set_auto_watermark_interval(1_000)
    environment.enable_checkpointing(
        config.checkpoint_interval_seconds * 1_000,
        CheckpointingMode.EXACTLY_ONCE,
    )

    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_settings = Configuration()
    checkpoint_settings.set_string("execution.checkpointing.storage", "filesystem")
    checkpoint_settings.set_string(
        "execution.checkpointing.dir",
        config.checkpoint_dir.resolve().as_uri(),
    )
    environment.configure(checkpoint_settings)

    checkpoint_config = environment.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(config.checkpoint_min_pause_seconds * 1_000)
    checkpoint_config.set_checkpoint_timeout(config.checkpoint_timeout_seconds * 1_000)
    checkpoint_config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )
    return environment


def _build_source(config: StreamingFeatureConfig) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.source_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("commit.offsets.on.checkpoint", "true")
        .set_property("partition.discovery.interval.ms", "30000")
        .set_property("client.id.prefix", "fraudstream-flink-features")
        .build()
    )


def _build_feature_window(
    events: Any,
    *,
    entity_type: str,
    key_selector: Any,
    late_output: OutputTag,
    config: StreamingFeatureConfig,
) -> tuple[Any, Any]:
    features = (
        events.key_by(key_selector, key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(config.window_minutes)))
        .allowed_lateness(config.allowed_lateness_minutes * 60 * 1_000)
        .side_output_late_data(late_output)
        .aggregate(
            FeatureAggregateFunction(entity_type),
            window_function=FeatureWindowFunction(entity_type),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
        .name(f"Build {entity_type} five-minute features")
        .uid(f"fraudstream-{entity_type}-features-5m-v1")
    )
    late_records = (
        features.get_side_output(late_output)
        .map(
            LateRecordFunction(f"{entity_type}_features_5m", config.window_minutes),
            output_type=Types.STRING(),
        )
        .name(f"Format late {entity_type} events")
        .uid(f"fraudstream-late-{entity_type}-features-v1")
    )
    return features, late_records


def _sink(stream: Any, topic: str, sink_name: str, config: StreamingFeatureConfig) -> None:
    transactional_id_prefix = f"fraudstream-{sink_name}-"
    if config.benchmark_profile != "none":
        transactional_id_prefix = (
            f"fraudstream-{config.benchmark_profile}-{sink_name}-"
        )
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(transactional_id_prefix)
        .set_property("transaction.timeout.ms", "900000")
        .build()
    )
    stream.sink_to(sink).name(f"Kafka sink: {topic}").uid(f"fraudstream-{sink_name}-sink-v1")


def _event_id(value: str) -> str:
    return json.loads(value)["event_id"]


def _customer_id(value: str) -> str:
    return json.loads(value)["customer_id"]


def _merchant_id(value: str) -> str:
    return json.loads(value)["merchant_id"]


def _jar_uri(path: Path) -> str:
    return path.resolve().as_uri()
