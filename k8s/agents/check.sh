#!/usr/bin/env bash
# Checks the agent platform end to end: the global model config, that ids and
# dates reach the model unmasked through the LLM gateway's PII guard with
# thinking off, that each agent sees only its own tools, that the agents' calls go
# through the A2A gateway, which refuses a caller without a key or outside its
# allow-list and hands out agent cards with its own address, that the coordinator
# reaches both sandboxed specialists through it in fresh sessions, that every
# agent answers its eval questions with the right tool calls and the stores' and
# the model's numbers, that the coordinator's questions are counted at the risk
# agent and at the A2A gateway and make one trace through both, that the MCP
# gateway counted each specialist's tool calls, that KEDA keeps two coordinator
# replicas and every worker for the sandboxed agents is ready, that Jaeger counted the
# calls at each agent, and that the registry holds the agents and their MCP
# servers.
#   ./k8s/agents/check.sh            the checks
#   ./k8s/agents/check.sh --load 3 --agent coordinator-agent   three questions at once, to watch KEDA add a replica
#   ./k8s/agents/check.sh --agent coordinator-agent            only that agent's eval questions
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
card=$(mktemp)
trap 'for pid in "${forwards[@]}"; do kill "$pid" 2>/dev/null; done; rm -f "$card"' EXIT
# The binary, not the function: a function in the background runs in a subshell, and killing that can leave kubectl listening.
forward() { command kubectl --context kind-fraudstream -n "$1" port-forward "svc/$2" "$3" >/dev/null & forwards+=($!); sleep 2; }
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
promql() {
  kubectl get --raw "$prom?query=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$1")" |
    python3 -c 'import json, sys; print(sum(float(r["value"][1]) for r in json.load(sys.stdin)["data"]["result"]))'
}

asked=
ask() {
  if [ -z "$asked" ]; then
    forward kagent kagent-controller 18083:8083
    forward fraudstream-apis context-api 18093:80
    forward fraudstream-apis inference-api 18092:80
    asked=1
  fi
  PYTHONPATH=agents/src uv run --project agents python agents/tools/check_agent.py --controller http://127.0.0.1:18083 \
    --context-api http://127.0.0.1:18093 --inference-api http://127.0.0.1:18092 "$@"
}
case "${1:-}" in
  --load|--agent) ask "$@"; exit ;;
esac

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

echo "==> the A2A gateway"
gateway=http://a2a-gateway.kagent.svc.cluster.local:8080
expect "kagent's proxy" "$gateway" "$(kubectl -n kagent get configmap kagent-controller -o jsonpath='{.data.PROXY_URL}')"
for agent in context-agent risk-agent; do
  expect "the $agent's tools through it" "$gateway/mcp" \
    "$(kubectl -n kagent get secret "$agent" -o jsonpath='{.data.config\.json}' | base64 -d |
      python3 -c 'import json, sys; print(json.load(sys.stdin)["http_tools"][0]["params"]["url"])')"
done
forward kagent a2a-gateway 18095:8080
a2a_key=$(kubectl -n kagent get secret a2a-key-a2a-check -o jsonpath='{.data.authorization}' | base64 -d)
# Sandboxed agents have no Service: kagent's controller serves them, on their own path.
status() {
  curl -s -o "$card" -w '%{http_code}' "localhost:18095/api/a2a-sandboxes/kagent/$1/.well-known/agent-card.json" \
    -H "x-kagent-host: kagent-controller.kagent" "${@:2}"
}
expect "no key" 401 "$(status context-agent)"
expect "a key, an agent off its list" 403 "$(status risk-agent -H "Authorization: $a2a_key")"
expect "a key, an agent on its list" 200 "$(status context-agent -H "Authorization: $a2a_key")"
expect "the card points at the gateway" yes \
  "$(grep -q 'localhost:18095' "$card" && ! grep -q 'kagent-controller.kagent:8083' "$card" && echo yes || echo no)"

echo "==> the coordinator"
expect "the agents it calls" "context-agent risk-agent" \
  "$(kubectl -n kagent get agent coordinator-agent -o jsonpath='{.spec.declarative.tools[*].agent.name}')"
expect "a fresh session per call" "true true" \
  "$(kubectl -n kagent get agent coordinator-agent -o jsonpath='{.spec.declarative.tools[*].isolateSessions}')"
expect "through the A2A gateway" "$gateway/api/a2a-sandboxes/kagent/context-agent $gateway/api/a2a-sandboxes/kagent/risk-agent" \
  "$(kubectl -n kagent get secret coordinator-agent -o jsonpath='{.data.config\.json}' | base64 -d |
    python3 -c 'import json, sys; print(*(agent["url"] for agent in json.load(sys.stdin)["remote_agents"]))')"

echo "==> questions to the agents"
ask --agent context-agent --agent risk-agent || failed=1
# Prometheus scrapes every 15 seconds, and a call is counted once its span ends.
sleep 30
before=$(promql 'sum(agent_calls_total{gen_ai_agent_name="risk_agent"})')
ask --agent coordinator-agent || failed=1
sleep 45
after=$(promql 'sum(agent_calls_total{gen_ai_agent_name="risk_agent"})')
expect "the coordinator's questions counted at the risk agent" yes \
  "$(python3 -c 'import sys; print("yes" if float(sys.argv[2]) - float(sys.argv[1]) >= 2 else "no")' "$before" "$after")"
expect "the coordinator's calls counted at the A2A gateway" yes \
  "$(python3 -c 'import sys; print("yes" if float(sys.argv[1]) > 0 else "no")' \
    "$(promql 'sum(agentgateway_requests_total{gateway="kagent/a2a-gateway", protocol="a2a", consumer="coordinator-agent"})')")"

echo "==> one trace from the coordinator to the risk agent"
forward observability jaeger 18096:16686
python3 - <<'PY' || failed=1
import json, urllib.request

traces = json.load(urllib.request.urlopen("http://localhost:18096/api/traces?service=coordinator_agent&limit=20"))["data"]
best = max(traces, key=lambda trace: len({process["serviceName"] for process in trace["processes"].values()}), default=None)
services = {process["serviceName"] for process in best["processes"].values()} if best else set()
risk = {span["operationName"] for span in best["spans"] if best["processes"][span["processID"]]["serviceName"] == "risk_agent"} if best else set()
print(f"        services: {sorted(services)}")
print(f"        risk agent's operations: {sorted(risk)}")
ok = ({"coordinator_agent", "a2a-gateway", "risk_agent"} <= services and "execute_tool score_payment" in risk
      and any(name.startswith("generate_content") for name in risk))
print(("  ok    " if ok else "  FAIL  ") + "coordinator, A2A gateway and risk agent in one trace")
raise SystemExit(0 if ok else 1)
PY

echo "==> replicas"
expect "coordinator-agent's scaler" True \
  "$(kubectl -n kagent get scaledobject coordinator-agent -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
expect "coordinator-agent at least two ready" yes \
  "$([ "$(kubectl -n kagent get deploy coordinator-agent -o jsonpath='{.status.readyReplicas}')" -ge 2 ] && echo yes || echo no)"
# The sandboxed agents' replicas are the workers that run their sessions, a fixed pool.
expect "the workers ready" "$(kubectl -n kagent get workerpool kagent-default -o jsonpath='{.spec.replicas}')" \
  "$(kubectl -n kagent get deploy kagent-default-deployment -o jsonpath='{.status.readyReplicas}')"

echo "==> the registry"
forward agentregistry agentregistry 18121:12121
expect "the context agent's record" yes \
  "$(curl -s localhost:18121/v0/agents/contextagent | grep -q '"Context agent"' && echo yes || echo no)"
expect "the risk agent's record" yes \
  "$(curl -s localhost:18121/v0/agents/riskagent | grep -q '"Risk agent"' && echo yes || echo no)"
expect "the coordinator's record" yes \
  "$(curl -s localhost:18121/v0/agents/coordinatoragent | grep -q '"Coordinator agent"' && echo yes || echo no)"
for server in fraud-context fraud-risk; do
  expect "the $server record" 200 "$(curl -s -o /dev/null -w '%{http_code}' "localhost:18121/v0/mcpservers/$server")"
done

echo "==> the counters"
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
for agent in ("context_agent", "risk_agent", "coordinator_agent"):
    ok &= agent in agents
    print(("  ok    " if agent in agents else "  FAIL  ") + f"calls to {agent} counted from its own spans")
sys.exit(0 if ok else 1)
PY

exit $failed
