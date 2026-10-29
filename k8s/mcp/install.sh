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

# A key is made once per consumer in Vault and kept. External Secrets copies each
# key, and the gateway's list of key hashes with the MCP servers each may use.
echo "==> API keys"
for consumer in context-agent risk-agent mcp-check; do ./infra/vault/new-key.sh "mcp/$consumer"; done
kubectl apply -f k8s/mcp/secrets.yaml >/dev/null
kubectl -n mcp wait externalsecret --all --for=condition=Ready --timeout=2m

echo "==> the MCP gateway and its policy"
kubectl apply -f k8s/mcp/gateway.yaml >/dev/null
kubectl -n mcp wait --for=condition=Programmed gateway/mcp-gateway --timeout=3m
kubectl apply -f k8s/mcp/policy.yaml -f k8s/observability/servicemonitors.yaml >/dev/null

echo "MCP gateway: http://mcp-gateway.mcp.svc.cluster.local:8080/mcp"
