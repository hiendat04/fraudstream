#!/usr/bin/env bash
# Installs the agent platform on the kind cluster: kagent, whose one global model
# config reaches Qwen3-4B through the LLM gateway, and the context and risk agents,
# each with its prompt from MLflow, its own key to the MCP gateway, and KEDA
# scaling it on calls in flight, all under one NetworkPolicy; then agentregistry,
# with the agents and their MCP servers published to it; and both UIs behind
# NGINX with HTTPS and the gateway login.
# Safe to run again. Run k8s/gateway/credentials.sh for kagent and agentregistry
# once first, so the login exists.
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

agents=(context-agent risk-agent)

echo "==> the agents' MCP keys"
for agent in "${agents[@]}"; do
  kubectl -n kagent create secret generic "mcp-key-$agent" \
    --from-literal=authorization="Bearer $(kubectl -n mcp get secret "mcp-key-$agent" -o jsonpath='{.data.key}' | base64 -d)" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

# Until the agents have CI/CD, the prompt in git is promoted as it is registered.
echo "==> the agents' prompts"
prompts() { (cd agents && PYTHONPATH=src MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://localhost:18085} uv run python -m fraudstream_agents.prompts "$@"); }
for agent in "${agents[@]}"; do
  prompts register "$agent" --promote
  prompts render "$agent" | kubectl apply -f - >/dev/null
done

echo "==> the agents"
kubectl apply -f k8s/agents/network.yaml >/dev/null
for agent in "${agents[@]}"; do kubectl apply -f "k8s/agents/$agent.yaml" >/dev/null; done
for agent in "${agents[@]}"; do kubectl -n kagent wait "agent/$agent" --for=condition=Ready --timeout=5m; done

echo "==> agent telemetry and the Agents dashboard"
kubectl apply -f k8s/observability/servicemonitors.yaml -f k8s/observability/dashboards/agents.yaml >/dev/null
for agent in "${agents[@]}"; do kubectl -n kagent wait "scaledobject/$agent" --for=condition=Ready --timeout=2m; done

echo "==> agentregistry ${AGENTREGISTRY_VERSION}"
kubectl create namespace agentregistry --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm --kube-context kind-fraudstream upgrade --install agentregistry oci://ghcr.io/agentregistry-dev/agentregistry/charts/agentregistry \
  --version "$AGENTREGISTRY_VERSION" -n agentregistry -f k8s/agents/agentregistry-values.yaml --wait --timeout 5m >/dev/null

# arctl, the registry's command line, fetched once and checked against its release's checksum.
echo "==> arctl ${AGENTREGISTRY_VERSION}"
arctl=${XDG_CACHE_HOME:-$HOME/.cache}/fraudstream/arctl-$AGENTREGISTRY_VERSION
if [ ! -x "$arctl" ]; then
  asset=arctl-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
  release=https://github.com/agentregistry-dev/agentregistry/releases/download/v$AGENTREGISTRY_VERSION
  mkdir -p "$(dirname "$arctl")"
  curl -fsSL -o "$arctl.download" "$release/$asset"
  [ "$(shasum -a 256 "$arctl.download" | cut -d' ' -f1)" = "$(curl -fsSL "$release/$asset.sha256" | cut -d' ' -f1)" ] \
    || { echo "arctl's checksum does not match its release" >&2; rm -f "$arctl.download"; exit 1; }
  chmod +x "$arctl.download" && mv "$arctl.download" "$arctl"
fi

echo "==> the registry records"
kubectl -n agentregistry port-forward svc/agentregistry 18121:12121 >/dev/null &
forward=$!
trap 'kill $forward 2>/dev/null' EXIT
for _ in $(seq 30); do curl -sf localhost:18121/health >/dev/null && break; sleep 1; done
"$arctl" --registry-url http://127.0.0.1:18121 apply -f k8s/agents/registry.yaml

echo "==> the UIs behind NGINX"
kubectl apply -f k8s/agents/ingress.yaml >/dev/null

echo "kagent UI:   https://kagent.fraudstream.localhost (gateway password)"
echo "registry UI: https://agentregistry.fraudstream.localhost (gateway password)"
