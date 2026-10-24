#!/usr/bin/env bash
# Checks the agent platform end to end: the global model config, that ids and
# dates reach the model unmasked through the LLM gateway's PII guard with
# thinking off, that each agent sees only its own tools, that the context and risk
# agents answer their eval questions with the right tool calls and the stores'
# and the model's numbers, that the gateway counted each agent's tool calls, that
# KEDA keeps two replicas of each, that Jaeger counted the calls at each agent,
# and that the registry holds the agents and their MCP servers.
#   ./k8s/agents/check.sh            the checks
#   ./k8s/agents/check.sh --load 3 --agent risk-agent   three questions at once, to watch KEDA add a replica
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

ask() {
  forward kagent kagent-controller 18083:8083
  forward fraudstream-apis context-api 18093:80
  forward fraudstream-apis inference-api 18092:80
  PYTHONPATH=agents/src uv run --project agents python agents/tools/check_agent.py --controller http://127.0.0.1:18083 \
    --context-api http://127.0.0.1:18093 --inference-api http://127.0.0.1:18092 "$@"
}
if [ "${1:-}" = --load ]; then
  ask "$@"
  exit
fi

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

echo "==> each agent's tools"
expect "fraud-context-tools" "get_chunk get_customer_features get_merchant_features" \
  "$(kubectl -n kagent get remotemcpserver fraud-context-tools -o jsonpath='{.status.discoveredTools[*].name}' | tr ' ' '\n' | sort | xargs)"
expect "fraud-risk-tools" "score_payment search_handbook" \
  "$(kubectl -n kagent get remotemcpserver fraud-risk-tools -o jsonpath='{.status.discoveredTools[*].name}' | tr ' ' '\n' | sort | xargs)"

echo "==> questions to the agents"
ask --agent context-agent --agent risk-agent || failed=1

echo "==> replicas"
for agent in context-agent risk-agent; do
  expect "$agent's scaler" True \
    "$(kubectl -n kagent get scaledobject "$agent" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
  expect "$agent at least two ready" yes \
    "$([ "$(kubectl -n kagent get deploy "$agent" -o jsonpath='{.status.readyReplicas}')" -ge 2 ] && echo yes || echo no)"
done

echo "==> the registry"
forward agentregistry agentregistry 18121:12121
expect "the context agent's record" yes \
  "$(curl -s localhost:18121/v0/agents/contextagent | grep -q '"Context agent"' && echo yes || echo no)"
expect "the risk agent's record" yes \
  "$(curl -s localhost:18121/v0/agents/riskagent | grep -q '"Risk agent"' && echo yes || echo no)"
for server in fraud-context fraud-risk; do
  expect "the $server record" 200 "$(curl -s -o /dev/null -w '%{http_code}' "localhost:18121/v0/mcpservers/$server")"
done

echo "==> the counters"
# Prometheus scrapes every 15 seconds.
sleep 30
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
python3 - "$prom" <<'PY' || failed=1
import json, subprocess, sys, urllib.parse


def query(promql):
    url = sys.argv[1] + "?query=" + urllib.parse.quote(promql)
    out = subprocess.run(["kubectl", "--context", "kind-fraudstream", "get", "--raw", url], capture_output=True, text=True, check=True)
    series = json.loads(out.stdout)["data"]["result"]
    for item in series:
        print(f"        {item['metric']} {item['value'][1]}")
    return series


ok = True
for consumer, wanted in (("context-agent", {"get_customer_features", "get_merchant_features"}),
                         ("risk-agent", {"score_payment", "search_handbook"})):
    series = query(f'sum by (consumer, resource) (agentgateway_mcp_requests_total{{method="tools/call", consumer="{consumer}"}})')
    tools = {item["metric"].get("resource") for item in series}
    passed = wanted <= tools
    ok &= passed
    print(("  ok    " if passed else "  FAIL  ") + f"tool calls counted for {consumer}: {sorted(t for t in tools if t)}")
agents = {item["metric"].get("gen_ai_agent_name") for item in query("sum by (gen_ai_agent_name, status_code) (agent_calls_total)")}
for agent in ("context_agent", "risk_agent"):
    ok &= agent in agents
    print(("  ok    " if agent in agents else "  FAIL  ") + f"calls to {agent} counted from its own spans")
sys.exit(0 if ok else 1)
PY

exit $failed
