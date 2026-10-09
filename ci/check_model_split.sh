#!/usr/bin/env bash
# Send 30 replayed payments through the gateway and check that the model
# versions answering match the InferenceService's split.
#   set -a && . ./.env && set +a
#   GATEWAY_ADDRESS=127.0.0.1 ./ci/check_model_split.sh
set -euo pipefail
cd "$(dirname "$0")/.."
: "${GATEWAY_USER:?set GATEWAY_USER}" "${GATEWAY_PASSWORD:?set GATEWAY_PASSWORD}"
. ci/gateway.sh

work=$(mktemp -d)
kubectl -n kserve-models get inferenceservice fraud-detection -o json > "$work/isvc.json"
head -30 api/tools/payments.jsonl | while IFS= read -r payment; do
  gateway_curl -s --max-time 30 -u "$GATEWAY_USER:$GATEWAY_PASSWORD" \
    -H 'Content-Type: application/json' -d "$payment" \
    https://inference.fraudstream.localhost/v1/predict || true
  echo
done > "$work/answers.jsonl"
uv run --no-project python api/tools/model_split.py "$work/isvc.json" "$work/answers.jsonl"
