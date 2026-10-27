#!/usr/bin/env bash
# Deploys one agent, gated by its eval questions. Registers the agent's prompt
# from git in MLflow under the alias candidate, renders that version into the
# agent's ConfigMap and applies the agent, waits until kagent runs both (the
# coordinator's pods rolled, a sandboxed agent's golden snapshot rebuilt), and
# asks the agent's eval questions. When they pass, it points production at the
# candidate and publishes to agentregistry. When anything from the deploy on
# fails, it renders the production prompt back, applies the agent from
# ROLLBACK_COMMIT when that commit has it (from git otherwise, so a SandboxAgent
# takes the production prompt's version), drops the candidate alias and exits 1.
#   ./cicd/deploy_agent.sh coordinator-agent                                     in Jenkins
#   MLFLOW_TRACKING_URI=http://localhost:18085 ./cicd/deploy_agent.sh context-agent   by hand
set -euo pipefail
cd "$(dirname "$0")/.."
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }
prompts() { (cd agents && PYTHONPATH=src MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://localhost:18085} uv run python -m fraudstream_agents.prompts "$@"); }

agent=${1:-}
case "$agent" in
  context-agent|risk-agent) specialist=yes kind=sandboxagent ;;
  coordinator-agent) specialist= kind=agent ;;
  *) echo "usage: $0 context-agent|risk-agent|coordinator-agent" >&2; exit 2 ;;
esac

# kagent copies the ConfigMap's prompt into the agent's config.
config_written() {
  [ "$(kubectl -n kagent get secret "$agent" -o jsonpath='{.data.config\.json}' | base64 -d | jq -r .instruction)" \
    = "$(kubectl -n kagent get configmap "$agent-prompt" -o json | jq -r '.data["system-message"]')" ]
}

# The template kagent made for this generation of the SandboxAgent, with its golden snapshot taken.
golden_ready() {
  kubectl -n kagent get actortemplates -l "kagent.dev/sandbox-agent=$agent" -o json |
    jq -e --arg generation "$1" '.items[] | select(.metadata.annotations["kagent.dev/desired-generation"] == $generation and .status.phase == "Ready")' >/dev/null
}

# Waits until kagent has reconciled the applied agent and its prompt, then for
# the pods to roll or, for a sandboxed agent, the golden snapshot fresh sessions start from.
rolled() {
  local generation
  generation=$(kubectl -n kagent get "$kind" "$agent" -o jsonpath='{.metadata.generation}') || return 1
  kubectl -n kagent wait "$kind/$agent" --for=jsonpath='{.status.observedGeneration}'="$generation" --timeout=2m >/dev/null || return 1
  kubectl -n kagent wait "$kind/$agent" --for=condition=Ready --timeout=10m >/dev/null || return 1
  for _ in $(seq 24); do config_written && break; sleep 5; done
  config_written || { echo "kagent did not write the new prompt into $agent's config" >&2; return 1; }
  if [ -z "$specialist" ]; then
    kubectl -n kagent rollout status "deploy/$agent" --timeout=5m
    return
  fi
  for _ in $(seq 120); do golden_ready "$generation" && return; sleep 5; done
  echo "no golden snapshot of $agent's generation $generation" >&2
  return 1
}

# Each step says how it failed: errexit is off inside a function called before ||.
deploy_and_check() {
  echo "==> $agent, running the candidate"
  prompts render "$agent" --alias candidate | kubectl apply -f - >/dev/null || return 1
  ./k8s/agents/apply.sh "$agent" < "k8s/agents/$agent.yaml" || return 1
  rolled || return 1
  echo "==> $agent's eval questions"
  ./k8s/agents/check.sh --agent "$agent"
}

rollback() {
  echo "==> rolling $agent back to its production prompt"
  prompts render "$agent" | kubectl apply -f - >/dev/null
  if [ -n "${ROLLBACK_COMMIT:-}" ] && git cat-file -e "$ROLLBACK_COMMIT:k8s/agents/$agent.yaml" 2>/dev/null; then
    git show "$ROLLBACK_COMMIT:k8s/agents/$agent.yaml" | ./k8s/agents/apply.sh "$agent"
  else
    # The agent as applied, but a SandboxAgent on the production prompt's version.
    ./k8s/agents/apply.sh "$agent" < "k8s/agents/$agent.yaml"
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
