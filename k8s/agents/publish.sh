#!/usr/bin/env bash
# Publishes the agents and their MCP servers to agentregistry from
# k8s/agents/registry.yaml, with arctl, the registry's command line, fetched once
# and checked against its release's checksum. Safe to run again: arctl applies
# each record as it is in the file.
#   ./k8s/agents/publish.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

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
