"""Writes payments into Memgraph and reads each device's ring features back."""

from __future__ import annotations

from dataclasses import dataclass

from fraudstream.graph.payments_transform import PAYMENT_MERGE

BATCH_SIZE = 5_000


@dataclass(frozen=True)
class RingFeatures:
    device_id: str
    customer_count: int
    fraud_customer_count: int
    ip_customer_count: int


# Payments with start <= at < end. The IP hop is a clause of its own, so a customer's
# own payment counts as sharing its IP; OPTIONAL keeps a row for a device with no IP.
RING_FEATURES = """
UNWIND $device_ids AS device_id
OPTIONAL MATCH (:Device {id: device_id})<-[:ON]-(p:Payment)<-[:MADE]-(c:Customer)
WHERE p.at >= $start AND p.at < $end
OPTIONAL MATCH (p)-[:FROM]->(ip:Ip)
OPTIONAL MATCH (ip)<-[:FROM]-(q:Payment)<-[:MADE]-(other:Customer)
WHERE q.at >= $start AND q.at < $end
WITH device_id,
     collect(DISTINCT c.id) AS customers,
     collect(DISTINCT CASE WHEN p.fraud THEN c.id END) AS fraud_customers,
     collect(DISTINCT other.id) AS ip_customers
RETURN device_id, size(customers) AS customer_count,
       size(fraud_customers) AS fraud_customer_count,
       size(ip_customers) AS ip_customer_count
"""

# The customers who paid on one device with start <= at < end, fraud first, at most 10.
device_neighbours_query = """
MATCH (:Device {id: $device_id})<-[:ON]-(p:Payment)<-[:MADE]-(c:Customer)
WHERE p.at >= $start AND p.at < $end
WITH c.id AS customer_id, count(p) AS payments, sum(CASE WHEN p.fraud THEN 1 ELSE 0 END) > 0 AS fraud
RETURN customer_id, payments, fraud
ORDER BY fraud DESC, payments DESC, customer_id
LIMIT 10
"""


def add_payments(session, rows: list[dict]) -> None:
    for start in range(0, len(rows), BATCH_SIZE):
        session.run(PAYMENT_MERGE, rows=rows[start:start + BATCH_SIZE]).consume()


def ring_features(session, device_ids: list[str], start: int, end: int) -> list[RingFeatures]:
    result = session.run(RING_FEATURES, device_ids=device_ids, start=start, end=end)
    return [RingFeatures(**record.data()) for record in result]
