#!/usr/bin/env bash
# Applies risk-agent-b: the risk agent made from risk-agent.yaml on small-model-config,
# so the two differ only in their model config. It takes the risk agent's prompt and
# prompt version, so run it again after the risk agent's prompt is deployed.
#   ./k8s/agents/risk-agent-b.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

command kubectl --context kind-fraudstream create --dry-run=client -o json -f k8s/agents/risk-agent.yaml |
  jq 'select(.kind == "SandboxAgent") | .metadata.name = "risk-agent-b" | .spec.declarative.modelConfig = "small-model-config"' |
  ./k8s/agents/apply.sh risk-agent
