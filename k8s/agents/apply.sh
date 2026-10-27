#!/usr/bin/env bash
# Applies an agent's YAML, read from stdin. A sandboxed agent's sessions start
# from a golden snapshot that keeps the prompt it was built with, so a
# SandboxAgent also gets its prompt's MLflow version, from the agent's ConfigMap,
# as PROMPT_VERSION: a new version is a new template, snapshotted on the new prompt.
#   ./k8s/agents/apply.sh context-agent < k8s/agents/context-agent.yaml
set -euo pipefail

kubectl() { command kubectl --context kind-fraudstream "$@"; }

prompt=$(kubectl -n kagent get configmap "$1-prompt" -o jsonpath='{.metadata.annotations.fraudstream\.dev/prompt}')
kubectl create --dry-run=client -o json -f - |
  jq -s --arg version "${prompt##*/}" '{apiVersion: "v1", kind: "List", items: map(
    if .kind == "SandboxAgent" then .spec.declarative.deployment.env =
      [(.spec.declarative.deployment.env // [])[] | select(.name != "PROMPT_VERSION")] + [{name: "PROMPT_VERSION", value: $version}]
    else . end)}' |
  kubectl apply -f - >/dev/null
