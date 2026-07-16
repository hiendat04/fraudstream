"""Unit tests for the pure contracts behind the PyFlink streaming job."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest import TestCase, main

from fraudstream.jobs.flink.transactions import (
    StreamingFeatureConfig,
    add_event_to_accumulator,
    build_alert_records,
    build_feature_record,
    build_late_record,
    create_feature_accumulator,
    event_business_fingerprint,
    event_time_window,
    extract_event_timestamp_ms,
    parse_transaction_message,
)


class FlinkStreamingTransactionsTest(TestCase):
    """Tests event validation, feature aggregation, and alerts without PyFlink."""

    def test_parse_transaction_message_normalizes_valid_event(self) -> None:
        raw_payload = json.dumps(_event_envelope())

        result = parse_transaction_message(raw_payload)

        self.assertTrue(result.is_valid)
        assert result.event is not None
        self.assertEqual(result.event["event_id"], "evt_stream_000000000001")
        self.assertEqual(result.event["amount"], "125.50")
        self.assertEqual(result.event["amount_cents"], 12_550)
        self.assertEqual(result.event["event_timestamp"], "2026-07-01T00:01:00.000Z")
        self.assertEqual(result.event["arrival_delay_seconds"], 120)

    def test_parse_transaction_message_quarantines_contract_errors(self) -> None:
        envelope = _event_envelope()
        del envelope["value"]["merchant_id"]
        envelope["headers"]["schema_version"] = "stream_v0"

        result = parse_transaction_message(json.dumps(envelope))

        self.assertFalse(result.is_valid)
        self.assertIn("missing_value_merchant_id", result.error_codes)
        self.assertIn("unsupported_schema_version", result.error_codes)

    def test_business_fingerprint_ignores_replay_metadata(self) -> None:
        original = _parsed_event()
        replay = copy.deepcopy(original)
        replay["produced_at"] = "2026-07-01T00:15:00.000Z"
        replay["arrival_delay_seconds"] = 840
        replay["source_sequence"] = 99
        replay["problem_flags"] = ["duplicate"]

        self.assertEqual(event_business_fingerprint(original), event_business_fingerprint(replay))

    def test_customer_window_builds_features_and_alerts(self) -> None:
        accumulator = create_feature_accumulator("customer")
        base_event = _parsed_event()
        for index in range(5):
            event = copy.deepcopy(base_event)
            event["event_id"] = f"evt_{index}"
            event["merchant_id"] = f"merchant_{index % 2}"
            event["device_id"] = f"device_{index % 3}"
            add_event_to_accumulator(accumulator, event)

        window_start_ms, window_end_ms = event_time_window(base_event["event_timestamp_ms"], 5)
        feature = build_feature_record(
            entity_type="customer",
            entity_id=base_event["customer_id"],
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            accumulator=accumulator,
            is_correction=False,
            emitted_at=datetime(2026, 7, 1, 0, 10, tzinfo=UTC),
        )
        alerts = build_alert_records(
            feature,
            StreamingFeatureConfig(customer_alert_txn_count=5, customer_alert_amount=Decimal("500.00")),
            detected_at=datetime(2026, 7, 1, 0, 10, tzinfo=UTC),
        )

        self.assertEqual(feature["txn_count"], 5)
        self.assertEqual(feature["amount_sum"], "627.50")
        self.assertEqual(feature["distinct_merchant_count"], 2)
        self.assertEqual(feature["distinct_device_count"], 3)
        self.assertEqual(
            {alert["rule_name"] for alert in alerts},
            {"customer_velocity_count", "customer_velocity_amount"},
        )

    def test_merchant_threshold_emits_deterministic_alert_id(self) -> None:
        accumulator = create_feature_accumulator("merchant")
        event = _parsed_event()
        for index in range(3):
            current = copy.deepcopy(event)
            current["customer_id"] = f"customer_{index}"
            add_event_to_accumulator(accumulator, current)
        window_start_ms, window_end_ms = event_time_window(event["event_timestamp_ms"], 5)
        feature = build_feature_record(
            entity_type="merchant",
            entity_id=event["merchant_id"],
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            accumulator=accumulator,
            is_correction=True,
            emitted_at=datetime(2026, 7, 1, 0, 10, tzinfo=UTC),
        )

        alerts = build_alert_records(
            feature,
            StreamingFeatureConfig(merchant_alert_txn_count=3),
            detected_at=datetime(2026, 7, 1, 0, 10, tzinfo=UTC),
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule_name"], "merchant_burst_count")
        self.assertEqual(
            alerts[0]["alert_id"],
            f"merchant_burst_count:{feature['feature_id']}",
        )
        self.assertTrue(alerts[0]["is_correction"])

    def test_late_record_preserves_window_and_source_identity(self) -> None:
        event = _parsed_event()

        record = build_late_record(
            event,
            window_name="customer_features_5m",
            window_minutes=5,
            recorded_at=datetime(2026, 7, 1, 1, 0, tzinfo=UTC),
        )

        self.assertEqual(record["window_start"], "2026-07-01T00:00:00.000Z")
        self.assertEqual(record["window_end"], "2026-07-01T00:05:00.000Z")
        self.assertEqual(record["reason"], "window_state_expired")
        self.assertIn("customer_features_5m", record["late_event_id"])

    def test_config_rejects_duplicate_topics(self) -> None:
        config = StreamingFeatureConfig(clean_topic="financial_transactions")

        with self.assertRaisesRegex(ValueError, "topics must be unique"):
            config.validate()

    def test_baseline_benchmark_profile_exposes_single_threaded_unchained_job(self) -> None:
        config = StreamingFeatureConfig(
            benchmark_profile="baseline",
            flink_ui_port=8082,
        ).apply_benchmark_profile()

        self.assertEqual(config.parallelism, 1)
        self.assertFalse(config.operator_chaining_enabled)
        self.assertTrue(config.flink_ui_enabled)
        self.assertEqual(config.flink_ui_port, 8082)
        self.assertEqual(config.job_name, "FraudStreamStreamingFeatures-baseline")

    def test_chained_profile_isolates_operator_chaining_change(self) -> None:
        config = StreamingFeatureConfig(
            benchmark_profile="chained"
        ).apply_benchmark_profile()

        self.assertEqual(config.parallelism, 1)
        self.assertTrue(config.operator_chaining_enabled)
        self.assertTrue(config.flink_ui_enabled)

    def test_optimized_benchmark_profile_enables_local_parallelism_and_chaining(self) -> None:
        config = StreamingFeatureConfig(
            benchmark_profile="optimized"
        ).apply_benchmark_profile()

        self.assertEqual(config.parallelism, 4)
        self.assertTrue(config.operator_chaining_enabled)
        self.assertTrue(config.flink_ui_enabled)
        self.assertEqual(config.to_dict()["benchmark"]["flink_ui_url"], "http://localhost:8081")

    def test_unknown_benchmark_profile_is_rejected(self) -> None:
        config = StreamingFeatureConfig(benchmark_profile="fastest")

        with self.assertRaisesRegex(ValueError, "benchmark_profile"):
            config.apply_benchmark_profile()

    def test_invalid_flink_ui_port_is_rejected(self) -> None:
        config = StreamingFeatureConfig(flink_ui_port=70_000)

        with self.assertRaisesRegex(ValueError, "flink_ui_port"):
            config.validate()

    def test_timestamp_extraction_returns_zero_for_invalid_json(self) -> None:
        self.assertEqual(extract_event_timestamp_ms("not-json"), 0)

    def test_compose_initializes_output_topics_with_broker_append_time(self) -> None:
        compose_text = Path("docker-compose.yml").read_text(encoding="utf-8")
        config = StreamingFeatureConfig()

        for topic in (
            config.clean_topic,
            config.invalid_topic,
            config.duplicate_topic,
            config.late_topic,
            config.customer_feature_topic,
            config.merchant_feature_topic,
            config.alert_topic,
        ):
            self.assertIn(topic, compose_text)
        self.assertIn("message.timestamp.type=LogAppendTime", compose_text)


def _parsed_event() -> dict:
    result = parse_transaction_message(json.dumps(_event_envelope()))
    assert result.event is not None
    return result.event


def _event_envelope() -> dict:
    return {
        "topic": "financial_transactions",
        "partition": 3,
        "partition_key": "cust_stream_00000001",
        "source_sequence": 1,
        "produced_at": "2026-07-01T00:03:00",
        "headers": {
            "event_type": "transaction.created",
            "schema_version": "stream_v1",
            "producer": "fraudstream_streaming_generator",
            "problem_flags": [],
        },
        "value": {
            "event_id": "evt_stream_000000000001",
            "transaction_id": "txn_stream_000000000001",
            "account_id": "acct_stream_00000001",
            "customer_id": "cust_stream_00000001",
            "merchant_id": "merch_stream_00000001",
            "merchant_category": "electronics",
            "amount": "125.50",
            "currency": "USD",
            "city": "Detroit",
            "channel": "online",
            "transaction_status": "approved",
            "is_fraud": False,
            "event_timestamp": "2026-07-01T00:01:00",
            "created_ts": "2026-07-01T00:03:00",
            "event_window_start": "2026-07-01T00:00:00",
            "event_window_end": "2026-07-01T00:05:00",
            "event_window_minutes": 5,
            "device_id": "dev_stream_00000001_01",
            "ip_address": "10.1.2.3",
            "authentication_method": "3ds",
            "risk_signal_version": "stream_v1",
        },
    }


if __name__ == "__main__":
    main()
