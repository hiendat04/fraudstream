#!/usr/bin/env bash
# Builds the MCP servers' image and deploys fraud-context and fraud-risk to the mcp
# namespace with the shared chart: a rolling update that never has fewer ready pods
# than wanted, and a rollback when the new pods do not become ready.
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
