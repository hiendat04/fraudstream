#!/usr/bin/env bash
# Installs the agent platform on the kind cluster: the A2A gateway, which every
# call an agent makes inside the cluster goes through, with a key, an allow-list
# and a rate limit for calls between agents; Agent Substrate, which runs
# sandboxed agents on a pool of gVisor workers; kagent, whose one global model config
# reaches Qwen3-4B through the LLM gateway, the context and risk agents, sandboxed
# on those workers, each with its own key to the MCP gateway, and the coordinator
# over them, with its key to the A2A gateway; every agent with its prompt from
# MLflow, KEDA scaling the coordinator on calls in flight, a fixed pool of three
# workers (KEDA cannot scale a WorkerPool, whose scale has no pod selector), and
# NetworkPolicies on the agents and the workers; then agentregistry, with the
# agents and their MCP servers published to it; and both UIs behind NGINX with
# HTTPS and the gateway login.
# Safe to run again. Needs Vault unsealed, External Secrets (k8s/secrets/install.sh),
# and the LLM and MCP installs, whose keys the agents use.
#   ./k8s/agents/install.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

kubectl create namespace kagent --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl create namespace agentregistry --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# A key is made once per caller in Vault and kept. External Secrets copies each key,
# the gateway's list of key hashes with the agents each may call, the agents' LLM
# and MCP keys made by the LLM and MCP installs, and both UIs' gateway login.
echo "==> the A2A keys, and every key and login the agents use"
for consumer in coordinator-agent a2a-check; do ./infra/vault/new-key.sh "a2a/$consumer"; done
kubectl apply -f k8s/agents/secrets.yaml >/dev/null
kubectl -n kagent wait externalsecret --all --for=condition=Ready --timeout=2m
kubectl -n agentregistry wait externalsecret --all --for=condition=Ready --timeout=2m

# Before kagent, whose proxy setting sends the agents' calls to it.
echo "==> the A2A gateway and its policy"
kubectl apply -f k8s/agents/a2a-gateway.yaml >/dev/null
kubectl -n kagent wait --for=condition=Programmed gateway/a2a-gateway --timeout=3m
kubectl apply -f k8s/agents/a2a-policy.yaml -f k8s/agents/network.yaml >/dev/null

# Before kagent, whose controller connects to it.
./k8s/agents/substrate.sh

echo "==> kagent ${KAGENT_VERSION}"
helm --kube-context kind-fraudstream upgrade --install kagent-crds oci://ghcr.io/kagent-dev/kagent/helm/kagent-crds \
  --version "$KAGENT_VERSION" -n kagent >/dev/null
helm --kube-context kind-fraudstream upgrade --install kagent oci://ghcr.io/kagent-dev/kagent/helm/kagent \
  --version "$KAGENT_VERSION" -n kagent -f k8s/agents/kagent-values.yaml --wait --timeout 10m >/dev/null

agents=(context-agent risk-agent)

# By hand the prompt in git is promoted as it is registered; cicd/deploy_agent.sh promotes
# it only after the agent's eval questions pass.
echo "==> the agents' prompts"
prompts() { (cd agents && PYTHONPATH=src MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://localhost:18085} uv run python -m fraudstream_agents.prompts "$@"); }
for agent in "${agents[@]}" coordinator-agent; do
  prompts register "$agent" --promote
  prompts render "$agent" | kubectl apply -f - >/dev/null
done

echo "==> the agents"
for agent in "${agents[@]}"; do ./k8s/agents/apply.sh "$agent" < "k8s/agents/$agent.yaml"; done
# The first golden snapshot of each takes about a minute.
for agent in "${agents[@]}"; do kubectl -n kagent wait "sandboxagent/$agent" --for=condition=Ready --timeout=10m; done
# The risk agent on a second model config, the B side of its A/B test; the split rests at 100/0.
kubectl apply -f k8s/agents/small-model-config.yaml >/dev/null
./k8s/agents/risk-agent-b.sh
kubectl -n kagent wait sandboxagent/risk-agent-b --for=condition=Ready --timeout=10m
# After the specialists are Ready: kagent refuses an Agent tool whose agent does not exist yet.
kubectl apply -f k8s/agents/coordinator-agent.yaml >/dev/null
kubectl -n kagent wait agent/coordinator-agent --for=condition=Ready --timeout=5m

echo "==> agent telemetry and the Agents dashboard"
kubectl apply -f k8s/observability/servicemonitors.yaml -f k8s/observability/dashboards/agents.yaml >/dev/null
kubectl -n kagent wait scaledobject/coordinator-agent --for=condition=Ready --timeout=2m

echo "==> agentregistry ${AGENTREGISTRY_VERSION}"
helm --kube-context kind-fraudstream upgrade --install agentregistry oci://ghcr.io/agentregistry-dev/agentregistry/charts/agentregistry \
  --version "$AGENTREGISTRY_VERSION" -n agentregistry -f k8s/agents/agentregistry-values.yaml --wait --timeout 5m >/dev/null

./k8s/agents/publish.sh

echo "==> the UIs behind NGINX"
kubectl apply -f k8s/agents/ingress.yaml >/dev/null

echo "kagent UI:   https://kagent.fraudstream.localhost (gateway password)"
echo "registry UI: https://agentregistry.fraudstream.localhost (gateway password)"
