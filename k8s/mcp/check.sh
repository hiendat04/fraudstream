#!/usr/bin/env bash
# Checks the MCP gateway end to end from this computer: keys, the tools each
# consumer sees and may call, a real call to each server, the rate limit, and that
# the gateway and the servers counted the calls in Prometheus.
#   ./k8s/mcp/check.sh                 the checks
#   ./k8s/mcp/check.sh --loop 120      call a tool for 120 s, counting failures
set -euo pipefail
cd "$(dirname "$0")/../.."
# --context on every call, not use-context: install.sh may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }
kubectl -n mcp wait --for=condition=Programmed gateway/mcp-gateway --timeout=60s >/dev/null

key() { kubectl -n mcp get secret "mcp-key-$1" -o jsonpath='{.data.key}' | base64 -d; }
# The binary, not the function: a function in the background runs in a subshell, and killing that can leave kubectl listening.
command kubectl --context kind-fraudstream -n mcp port-forward svc/mcp-gateway 18090:8080 >/dev/null &
forward=$!
trap 'kill $forward' EXIT
sleep 2

CONTEXT_AGENT_KEY=$(key context-agent) RISK_AGENT_KEY=$(key risk-agent) MCP_CHECK_KEY=$(key mcp-check) \
  uv run --project mcp_servers python mcp_servers/tools/check_gateway.py http://127.0.0.1:18090/mcp "$@"
[ "$#" -gt 0 ] && exit 0

# Prometheus scrapes every 15 seconds.
sleep 30
prom=/api/v1/namespaces/observability/services/kps-prometheus:9090/proxy/api/v1/query
uv run --project mcp_servers python - "$prom" <<'PY'
import json, subprocess, sys, urllib.parse
bad = 0
for query in ("sum by (consumer) (agentgateway_mcp_requests_total)",
              "sum by (server, tool, outcome) (mcp_tool_calls_total)"):
    url = sys.argv[1] + "?query=" + urllib.parse.quote(query)
    out = json.loads(subprocess.run(["kubectl", "--context", "kind-fraudstream", "get", "--raw", url], capture_output=True, text=True, check=True).stdout)
    series = out["data"]["result"]
    print(("ok  " if series else "FAIL") + f"  {query}: {len(series)} series")
    for item in series:
        print(f"        {item['metric']} {item['value'][1]}")
    bad += not series
sys.exit(1 if bad else 0)
PY
