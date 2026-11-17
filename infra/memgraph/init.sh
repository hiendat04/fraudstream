#!/bin/sh
# Indexes and the payment stream, made once. Memgraph keeps both across restarts,
# and a --init-file that creates the stream again crashes it on the next start.
set -eu
run() { echo "$1" | mgconsole --host memgraph --port 7687; }

for label in Customer Payment Device Ip; do
  run "CREATE INDEX ON :$label(id);" || true  # an existing index is not a failure
done

if run "SHOW STREAMS;" | grep -q payments; then
  echo "stream payments already exists"
else
  run "CREATE KAFKA STREAM payments TOPICS financial_transactions TRANSFORM payments_transform.payment CONSUMER_GROUP fraudstream-memgraph BOOTSTRAP_SERVERS 'kafka:29092';"
  run "START STREAM payments;"
fi
run "SHOW STREAMS;"
