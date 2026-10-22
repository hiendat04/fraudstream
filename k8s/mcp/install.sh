#!/usr/bin/env bash
# Builds the MCP servers' image and deploys fraud-context and fraud-risk to the mcp
# namespace with the shared chart: a rolling update that never has fewer ready pods
# than wanted, and a rollback when the new pods do not become ready. Then the MCP
# gateway in front of them, a key per consumer and the policy. Safe to run again:
# keys that exist are kept.
#   ./k8s/mcp/install.sh             builds the image tagged with the commit
#   TAG=v2 ./k8s/mcp/install.sh      another tag, to watch a rolling update
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl config use-context kind-fraudstream >/dev/null
TAG=${TAG:-$(git rev-parse --short=7 HEAD)}

echo "==> image fraudstream-mcp:$TAG"
docker build -f k8s/Dockerfile.mcp -t "fraudstream-mcp:$TAG" .
kind load docker-image "fraudstream-mcp:$TAG" --name fraudstream

echo "==> MCP servers"
kubectl create namespace mcp --dry-run=client -o yaml | kubectl apply -f - >/dev/null
for server in fraud-context fraud-risk; do
  helm upgrade --install "$server" k8s/charts/fraudstream-api -n mcp -f "k8s/mcp/$server.yaml" \
    --set-string image.tag="$TAG" --rollback-on-failure --timeout 3m
done

# A key is made once per consumer and kept. The gateway stores only its hash, with
# the consumer's name and the MCP servers whose tools it may see and call.
echo "==> API keys"
entry() {
  local consumer=$1 servers=$2 key hash
  kubectl -n mcp get secret "mcp-key-$consumer" >/dev/null 2>&1 \
    || kubectl -n mcp create secret generic "mcp-key-$consumer" --from-literal=key="$(openssl rand -hex 24)" >/dev/null
  key=$(kubectl -n mcp get secret "mcp-key-$consumer" -o jsonpath='{.data.key}' | base64 -d)
  hash=$(printf %s "$key" | shasum -a 256 | cut -d' ' -f1)
  printf '{"keyHash":"sha256:%s","metadata":{"consumer":"%s","servers":%s}}' "$hash" "$consumer" "$servers"
}
kubectl -n mcp create secret generic mcp-api-keys \
  --from-literal=context-agent="$(entry context-agent '["fraud-context"]')" \
  --from-literal=risk-agent="$(entry risk-agent '["fraud-risk"]')" \
  --from-literal=mcp-check="$(entry mcp-check '["fraud-context","fraud-risk"]')" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> the MCP gateway and its policy"
kubectl apply -f k8s/mcp/gateway.yaml >/dev/null
kubectl -n mcp wait --for=condition=Programmed gateway/mcp-gateway --timeout=3m
kubectl apply -f k8s/mcp/policy.yaml -f k8s/observability/servicemonitors.yaml >/dev/null

echo "MCP gateway: http://mcp-gateway.mcp.svc.cluster.local:8080/mcp"
