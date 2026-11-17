"""How a payment is written into the graph, and Memgraph's transform for the payment stream.

Memgraph loads this one file as a query module, so it imports nothing from the package.
"""

import json
from datetime import datetime, timezone

try:
    import mgp
except ImportError:  # outside Memgraph: the batch load uses only PAYMENT_MERGE
    mgp = None

# A null device or IP adds no edge: in-store payments have neither.
PAYMENT_MERGE = """
UNWIND $rows AS row
MERGE (c:Customer {id: row.customer_id})
MERGE (p:Payment {id: row.transaction_id})
SET p.at = row.at, p.fraud = row.fraud
MERGE (c)-[:MADE]->(p)
FOREACH (_ IN CASE WHEN row.device_id IS NULL THEN [] ELSE [1] END |
  MERGE (d:Device {id: row.device_id})
  MERGE (p)-[:ON]->(d))
FOREACH (_ IN CASE WHEN row.ip_address IS NULL THEN [] ELSE [1] END |
  MERGE (i:Ip {id: row.ip_address})
  MERGE (p)-[:FROM]->(i))
"""


def payment_row(raw: bytes) -> dict | None:
    """One `financial_transactions` envelope as a PAYMENT_MERGE row, or None if it can't be read."""
    try:
        value = json.loads(raw)["value"]
        at = datetime.fromisoformat(value["event_timestamp"])
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        if not value["transaction_id"] or not value["customer_id"]:
            return None
        return {
            "transaction_id": value["transaction_id"],
            "customer_id": value["customer_id"],
            "device_id": value.get("device_id", None),
            "ip_address": value.get("ip_address", None),
            "at": int(at.timestamp()),
            "fraud": bool(value.get("is_fraud")),
        }
    except (ValueError, KeyError, TypeError):
        return None  # a bad message must not stop the stream


if mgp is not None:
    @mgp.transformation
    def payment(messages: mgp.Messages) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
        rows = (payment_row(messages.message_at(i).payload()) for i in range(messages.total_messages()))
        return [mgp.Record(query=PAYMENT_MERGE, parameters={"rows": [row]}) for row in rows if row]
