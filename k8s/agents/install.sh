#!/usr/bin/env bash
# Installs the agent platform on the kind cluster: kagent, whose one global model
# config reaches Qwen3-4B through the LLM gateway, and the context agent, with its
# prompt from MLflow and its own key to the MCP gateway. Safe to run again.
#   ./k8s/agents/install.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

kubectl create namespace kagent --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> the agents' LLM key"
kubectl -n kagent create secret generic kagent-llm-key \
  --from-literal=key="$(kubectl -n llm get secret llm-key-agents -o jsonpath='{.data.key}' | base64 -d)" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> kagent ${KAGENT_VERSION}"
helm --kube-context kind-fraudstream upgrade --install kagent-crds oci://ghcr.io/kagent-dev/kagent/helm/kagent-crds \
  --version "$KAGENT_VERSION" -n kagent >/dev/null
helm --kube-context kind-fraudstream upgrade --install kagent oci://ghcr.io/kagent-dev/kagent/helm/kagent \
  --version "$KAGENT_VERSION" -n kagent -f k8s/agents/kagent-values.yaml --wait --timeout 10m >/dev/null

echo "==> the context agent's MCP key"
kubectl -n kagent create secret generic mcp-key-context-agent \
  --from-literal=authorization="Bearer $(kubectl -n mcp get secret mcp-key-context-agent -o jsonpath='{.data.key}' | base64 -d)" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Until the agents have CI/CD, the prompt in git is promoted as it is registered.
echo "==> the context agent's prompt"
prompts() { (cd agents && PYTHONPATH=src MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://localhost:18085} uv run python -m fraudstream_agents.prompts "$@"); }
prompts register context-agent --promote
prompts render context-agent | kubectl apply -f - >/dev/null

echo "==> the context agent"
kubectl apply -f k8s/agents/context-agent.yaml >/dev/null
kubectl -n kagent wait agent/context-agent --for=condition=Ready --timeout=5m

echo "kagent UI: kubectl -n kagent port-forward svc/kagent-ui 18084:8080"
