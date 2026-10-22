#!/usr/bin/env bash
# Installs the agent platform on the kind cluster: kagent, whose one global model
# config reaches Qwen3-4B through the LLM gateway. Safe to run again.
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

echo "kagent UI: kubectl -n kagent port-forward svc/kagent-ui 18084:8080"
