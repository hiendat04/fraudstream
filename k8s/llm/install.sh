#!/usr/bin/env bash
# Installs the LLM inference platform on the kind cluster: the Gateway API and
# Inference Extension CRDs, agentgateway, the model cache, the llama.cpp model
# servers, an llm-d endpoint picker per model, the Gateway with its models, and the
# policy on it. Safe to run again: keys that exist are kept.
#   ./k8s/llm/install.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
kubectl config use-context kind-fraudstream >/dev/null

# CRDs go on separately from their charts: Helm installs CRDs once and never
# updates them.
echo "==> Gateway API and Inference Extension CRDs"
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml" >/dev/null
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/${INFERENCE_EXTENSION_VERSION}/manifests.yaml" >/dev/null

echo "==> agentgateway ${AGENTGATEWAY_VERSION}"
helm upgrade --install agentgateway-crds oci://cr.agentgateway.dev/charts/agentgateway-crds \
  --version "$AGENTGATEWAY_VERSION" -n agentgateway-system --create-namespace >/dev/null
helm upgrade --install agentgateway oci://cr.agentgateway.dev/charts/agentgateway \
  --version "$AGENTGATEWAY_VERSION" -n agentgateway-system \
  -f k8s/llm/agentgateway-values.yaml --wait --timeout 5m >/dev/null

echo "==> model cache (first run downloads about 6 GB)"
kubectl create namespace llm --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# A finished Job can't be changed, so it is replaced each run; it skips files it has.
kubectl -n llm delete job fetch-models --ignore-not-found >/dev/null
kubectl apply -f k8s/llm/models/fetch.yaml >/dev/null
kubectl -n llm wait --for=condition=complete job/fetch-models --timeout=45m

echo "==> model servers"
kubectl apply -f k8s/llm/models/qwen3-4b.yaml -f k8s/llm/models/qwen3-1-7b.yaml >/dev/null
kubectl -n llm rollout status deploy/qwen3-4b --timeout=10m

echo "==> the Gateway"
kubectl apply -f k8s/llm/gateway.yaml >/dev/null
kubectl -n llm wait --for=condition=Programmed gateway/llm-gateway --timeout=3m

echo "LLM gateway: http://llm-gateway.llm.svc.cluster.local:8080/v1"
