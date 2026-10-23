#!/usr/bin/env bash
# Checks the agent platform end to end: the global model config, that ids and
# dates reach the model unmasked through the LLM gateway's PII guard with
# thinking off, that the context agent sees only its own tools, that it answers
# with numbers from the store, and that the gateway counted its tool calls.
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
  -d '{"model":"qwen3-4b","max_tokens":40,"temperature":0,"messages":[{"role":"user","content":"Repeat exactly, and nothing else: cust_00090571 merch_00011503 2026-06-22"}]}' |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["choices"][0]["message"]["content"])' 2>&1 || true)
echo "        reply: $reply"
expect "customer id unmasked" yes "$([[ $reply == *cust_00090571* ]] && echo yes || echo no)"
expect "merchant id unmasked" yes "$([[ $reply == *merch_00011503* ]] && echo yes || echo no)"
expect "date unmasked" yes "$([[ $reply == *2026-06-22* ]] && echo yes || echo no)"
expect "no thinking in the reply" yes "$([[ $reply != *"<think>"* ]] && echo yes || echo no)"

echo "==> the context agent's tools"
expect "tools it sees" "get_chunk get_customer_features get_merchant_features" \
  "$(kubectl -n kagent get remotemcpserver fraud-context-tools -o jsonpath='{.status.discoveredTools[*].name}' | tr ' ' '\n' | sort | xargs)"

    echo "==> questions to the context agent"
forward kagent kagent-controller 18083:8083
forward fraudstream-apis context-api 18093:80
PYTHONPATH=agents/src uv run --project agents python agents/tools/check_agent.py \
  --controller http://127.0.0.1:18083/api/a2a/kagent/context-agent/ --context-api http://127.0.0.1:18093 || failed=1

echo "==> the counters"
# Prometheus scrapes every 15 seconds.
sleep 30
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
python3 - "$prom" <<'PY' || failed=1
import json, subprocess, sys, urllib.parse
query = 'sum by (consumer, resource) (agentgateway_mcp_requests_total{method="tools/call", consumer="context-agent"})'
url = sys.argv[1] + "?query=" + urllib.parse.quote(query)
out = json.loads(subprocess.run(["kubectl", "--context", "kind-fraudstream", "get", "--raw", url], capture_output=True, text=True, check=True).stdout)
series = out["data"]["result"]
for item in series:
    print(f"        {item['metric']} {item['value'][1]}")
tools = {item["metric"].get("resource") for item in series}
ok = {"get_customer_features", "get_merchant_features"} <= tools
print(("  ok    " if ok else "  FAIL  ") + f"tool calls counted for context-agent: {sorted(t for t in tools if t)}")
sys.exit(0 if ok else 1)
PY

exit $failed
