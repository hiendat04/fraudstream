#!/usr/bin/env bash
# Sends one-token requests to the virtual model fraud-detection-llm and counts which version
# answered each, next to the live weights. The candidate's model server must be up.
# Exits 1 on any reply other than 200, or when a version with a weight never answered.
#   ./k8s/llm/check-split.sh [requests]
set -euo pipefail

kubectl() { command kubectl --context kind-fraudstream "$@"; }
requests=${1:-60}

key=$(kubectl -n llm get secret llm-key-benchmark -o jsonpath='{.data.key}' | base64 -d)
body=$(mktemp)

command kubectl --context kind-fraudstream -n llm port-forward svc/llm-gateway 18097:8080 >/dev/null &
forward=$!
trap 'kill $forward 2>/dev/null; wait $forward 2>/dev/null || true; rm -f "$body"' EXIT
sleep 2

# Each target names an AgentgatewayModel; its answers carry that model's own name.
weights=$(kubectl -n llm get agentgatewaymodel fraud-detection-llm -o json | jq -r '.spec.virtualModel.weighted.targets[] | "\(.modelRef.name) \(.weight // 1)"' |
  while read -r target weight; do
    echo "$(kubectl -n llm get agentgatewaymodel "$target" -o jsonpath='{.spec.match.model}') $weight"
  done)
total=$(awk '{sum += $2} END {print sum}' <<<"$weights")

answered=() failed=0
for _ in $(seq "$requests"); do
  status=$(curl -s -o "$body" -w '%{http_code}' localhost:18097/v1/chat/completions \
    -H "Authorization: Bearer $key" -H 'Content-Type: application/json' \
    -d '{"model":"fraud-detection-llm","max_tokens":1,"messages":[{"role":"user","content":"Say ok. /no_think"}]}')
  if [ "$status" = 200 ]; then
    answered+=("$(jq -r .model "$body")")
  else
    [ "$failed" -gt 0 ] || echo "  first failure, $status: $(head -c 200 "$body")"
    failed=$((failed + 1))
  fi
done

echo "==> fraud-detection-llm, $requests requests, $failed failed"
ok=$((failed == 0))
while read -r model weight; do
  count=$(printf '%s\n' ${answered[@]+"${answered[@]}"} | grep -cx "$model" || true)
  printf '  %-12s weight %3d%%   answered %3d (%d%%)\n' "$model" $((weight * 100 / total)) "$count" $((count * 100 / requests))
  [ "$count" -gt 0 ] || ok=0
done <<<"$weights"
[ "$ok" = 1 ]
