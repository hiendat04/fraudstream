#!/usr/bin/env bash
# Deploys one agent, gated by its eval questions. Registers the agent's prompt
# from git in MLflow under the alias candidate, renders that version into the
# agent's ConfigMap and applies the Agent, waits until kagent has rolled the pods
# onto both, and asks the agent's eval questions. When they pass, it points
# production at the candidate and publishes to agentregistry. When anything from
# the deploy on fails, it renders the production prompt back, applies the Agent
# from ROLLBACK_COMMIT when that commit has it, drops the candidate alias and exits 1.
#   ./cicd/deploy_agent.sh coordinator-agent                                     in Jenkins
#   MLFLOW_TRACKING_URI=http://localhost:18085 ./cicd/deploy_agent.sh context-agent   by hand
set -euo pipefail
cd "$(dirname "$0")/.."
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }
prompts() { (cd agents && PYTHONPATH=src MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://localhost:18085} uv run python -m fraudstream_agents.prompts "$@"); }

agent=${1:-}
case "$agent" in
  context-agent|risk-agent) specialist=yes ;;
  coordinator-agent) specialist= ;;
  *) echo "usage: $0 context-agent|risk-agent|coordinator-agent" >&2; exit 2 ;;
esac
tools_url=http://a2a-gateway.kagent.svc.cluster.local:8080/mcp

# kagent copies the ConfigMap's prompt into the agent's config, then rolls the pods onto it.
config_written() {
  [ "$(kubectl -n kagent get secret "$agent" -o jsonpath='{.data.config\.json}' | base64 -d | jq -r .instruction)" \
    = "$(kubectl -n kagent get configmap "$agent-prompt" -o json | jq -r '.data["system-message"]')" ]
}

# Waits until kagent has reconciled the applied Agent and its prompt, then for the pods to roll.
rolled() {
  local generation
  generation=$(kubectl -n kagent get agent "$agent" -o jsonpath='{.metadata.generation}') || return 1
  kubectl -n kagent wait "agent/$agent" --for=jsonpath='{.status.observedGeneration}'="$generation" --timeout=2m >/dev/null || return 1
  kubectl -n kagent wait "agent/$agent" --for=condition=Ready --timeout=5m >/dev/null || return 1
  for _ in $(seq 24); do config_written && break; sleep 5; done
  config_written || { echo "kagent did not write the new prompt into $agent's config" >&2; return 1; }
  kubectl -n kagent rollout status "deploy/$agent" --timeout=5m
}

# The pods, not terminating, that loaded their tools from another address than the A2A gateway.
stale_pods() {
  local pod
  for pod in $(kubectl -n kagent get pods -l "app.kubernetes.io/name=$agent" -o json |
               jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name'); do
    [ "$(kubectl -n kagent logs "$pod" | jq -Rr 'fromjson? | select(.msg == "Adding HTTP MCP tool") | .url' | tail -1)" = "$tools_url" ] \
      || echo "$pod"
  done
}

# Each step says how it failed: errexit is off inside a function called before ||.
deploy_and_check() {
  echo "==> $agent, running the candidate"
  prompts render "$agent" --alias candidate | kubectl apply -f - >/dev/null || return 1
  kubectl apply -f "k8s/agents/$agent.yaml" >/dev/null || return 1
  rolled || return 1
  if [ -n "$specialist" ] && [ -n "$(stale_pods)" ]; then
    # A pod can start before kagent writes the new config and keep the old one under the same hash.
    echo "==> restarting $agent: $(stale_pods | xargs) loaded old tool addresses"
    kubectl -n kagent rollout restart "deploy/$agent" >/dev/null || return 1
    kubectl -n kagent rollout status "deploy/$agent" --timeout=5m || return 1
    [ -z "$(stale_pods)" ] || { echo "still on old tool addresses: $(stale_pods | xargs)" >&2; return 1; }
  fi
  echo "==> $agent's eval questions"
  ./k8s/agents/check.sh --agent "$agent"
}

rollback() {
  echo "==> rolling $agent back to its production prompt"
  prompts render "$agent" | kubectl apply -f - >/dev/null
  if [ -n "${ROLLBACK_COMMIT:-}" ] && git cat-file -e "$ROLLBACK_COMMIT:k8s/agents/$agent.yaml" 2>/dev/null; then
    git show "$ROLLBACK_COMMIT:k8s/agents/$agent.yaml" | kubectl apply -f - >/dev/null
  fi
  rolled || true
  prompts unalias "$agent" --alias candidate
  echo "rolled back $agent to its production prompt"
  exit 1
}

if [ -n "$specialist" ]; then
  echo "==> $agent's MCP key"
  kubectl -n kagent create secret generic "mcp-key-$agent" \
    --from-literal=authorization="Bearer $(kubectl -n mcp get secret "mcp-key-$agent" -o jsonpath='{.data.key}' | base64 -d)" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

echo "==> $agent's prompt, as the candidate"
prompts register "$agent" --alias candidate

deploy_and_check || rollback

echo "==> $agent's candidate becomes production"
prompts promote "$agent" --from candidate
./k8s/agents/publish.sh
