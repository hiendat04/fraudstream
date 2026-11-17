"""How a payment is written into the graph.

Memgraph loads this one file as a query module, so it imports nothing from the package.
"""

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
