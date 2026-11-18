"""The Kafka transform Memgraph runs on every payment message, with a stand-in for `mgp`."""

from __future__ import annotations

import importlib
import json
import sys
import types
from unittest import TestCase, main


class Message:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def payload(self) -> bytes:
        return self._payload


class Messages:
    def __init__(self, *payloads: bytes) -> None:
        self._messages = [Message(payload) for payload in payloads]

    def total_messages(self) -> int:
        return len(self._messages)

    def message_at(self, index: int) -> Message:
        return self._messages[index]


class Record:
    def __init__(self, **fields) -> None:
        self.__dict__.update(fields)


def _fake_mgp() -> types.ModuleType:
    mgp = types.ModuleType("mgp")
    mgp.transformation = lambda function: function
    mgp.Record = Record
    mgp.Messages = Messages
    mgp.Map = dict
    mgp.Nullable = {mgp.Map: mgp.Map}  # mgp.Nullable[mgp.Map] only has to resolve
    return mgp


def envelope(**value) -> bytes:
    fields = {"transaction_id": "txn_1", "customer_id": "cust_1", "device_id": "dev_stream_ring_0001",
              "ip_address": "172.20.0.7", "is_fraud": True, "event_timestamp": "2026-06-22T23:59:21Z",
              "amount": 12.5}
    fields.update(value)
    return json.dumps({"headers": {"schema_version": "v1"}, "value": fields}).encode()


class PaymentsTransformTest(TestCase):
    def setUp(self) -> None:
        self.addCleanup(sys.modules.pop, "mgp", None)
        sys.modules["mgp"] = _fake_mgp()
        self.transform = importlib.reload(importlib.import_module("fraudstream.graph.payments_transform"))

    def test_an_envelope_becomes_one_payment_row(self) -> None:
        (record,) = self.transform.payment(Messages(envelope()))

        self.assertEqual(record.query, self.transform.PAYMENT_MERGE)
        self.assertEqual(record.parameters["rows"][0], {
            "transaction_id": "txn_1", "customer_id": "cust_1", "device_id": "dev_stream_ring_0001",
            "ip_address": "172.20.0.7", "at": 1782172761, "fraud": True,
        })

    def test_the_generators_offset_timestamp_reads_the_same(self) -> None:
        (record,) = self.transform.payment(Messages(envelope(event_timestamp="2026-06-22T23:59:21+00:00")))

        self.assertEqual(record.parameters["rows"][0]["at"], 1782172761)

    def test_a_blank_device_becomes_none(self) -> None:
        (record,) = self.transform.payment(Messages(envelope(device_id="", ip_address="")))

        row = record.parameters["rows"][0]
        self.assertEqual((row["device_id"], row["ip_address"]), (None, None))

    def test_an_unparseable_message_is_skipped(self) -> None:
        records = self.transform.payment(Messages(b"not json", envelope(customer_id=None), envelope()))

        self.assertEqual(len(records), 1)


if __name__ == "__main__":
    main()
