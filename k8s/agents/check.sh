#!/usr/bin/env bash
# Checks the agent platform end to end: the global model config,
# and that an id with eight digits reaches the model unmasked through the
# LLM gateway's PII guard, with thinking off.
#   ./k8s/agents/check.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
# --context on every call, not use-context: install.sh may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

failed=0
expect() {
  if [ "$3" = "$2" ]; then
    echo "  ok    $1: $3"
  else
    echo "  FAIL  $1: got '$3', wanted '$2'"
    failed=1
  fi
}
forwards=()
trap 'for pid in "${forwards[@]}"; do kill "$pid" 2>/dev/null; done' EXIT
forward() { kubectl -n "$1" port-forward "svc/$2" "$3" >/dev/null & forwards+=($!); sleep 2; }

echo "==> the global model config"
expect "model and where it is reached" "qwen3-4b http://llm-gateway.llm.svc.cluster.local:8080/v1" \
  "$(kubectl -n kagent get modelconfig default-model-config -o jsonpath='{.spec.model} {.spec.openAI.baseUrl}')"
expect "controller" ready \
  "$(kubectl -n kagent rollout status deploy/kagent-controller --timeout=60s >/dev/null 2>&1 && echo ready || echo 'not ready')"

echo "==> ids through the LLM gateway"
forward llm llm-gateway 18094:8080
key=$(kubectl -n llm get secret llm-key-agents -o jsonpath='{.data.key}' | base64 -d)
reply=$(curl -s --max-time 120 localhost:18094/v1/chat/completions -H "Authorization: Bearer $key" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3-4b","max_tokens":40,"temperature":0,"messages":[{"role":"user","content":"Repeat exactly, and nothing else: cust_00090571 merch_00011503"}]}' |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["choices"][0]["message"]["content"])' 2>&1 || true)
echo "        reply: $reply"
expect "customer id unmasked" yes "$([[ $reply == *cust_00090571* ]] && echo yes || echo no)"
expect "merchant id unmasked" yes "$([[ $reply == *merch_00011503* ]] && echo yes || echo no)"
expect "no thinking in the reply" yes "$([[ $reply != *"<think>"* ]] && echo yes || echo no)"

exit $failed
