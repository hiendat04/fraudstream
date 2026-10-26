#!/usr/bin/env bash
# Installs Agent Substrate on the kind cluster, the runtime that runs sandboxed
# agents as gVisor actors on a pool of pre-warmed workers: its CRDs, then its
# control plane, router, node agent, Valkey and RustFS in ate-system. RustFS,
# which keeps the actors' snapshots, gets a key pair made once and kept, in
# place of the chart's default login. Valkey is patched to announce its pod's
# current IP: on the 0.0.9 chart a restarted Valkey pod keeps announcing its old
# one, and sandboxed agents then never become ready. kagent's chart makes the
# worker pool. Safe to run again.
#   ./k8s/agents/substrate.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
# --context on every call, not use-context: another script may be writing the kubeconfig at the same time.
kubectl() { command kubectl --context kind-fraudstream "$@"; }

kubectl create namespace ate-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> RustFS keys"
kubectl -n ate-system get secret substrate-rustfs >/dev/null 2>&1 \
  || kubectl -n ate-system create secret generic substrate-rustfs \
    --from-literal=accessKey="$(openssl rand -hex 12)" --from-literal=secretKey="$(openssl rand -hex 24)" >/dev/null
rustfs() { kubectl -n ate-system get secret substrate-rustfs -o jsonpath="{.data.$1}" | base64 -d; }

echo "==> Agent Substrate ${SUBSTRATE_VERSION}"
helm --kube-context kind-fraudstream upgrade --install substrate-crds oci://ghcr.io/kagent-dev/substrate/helm/substrate-crds \
  --version "$SUBSTRATE_VERSION" -n ate-system --wait >/dev/null
helm --kube-context kind-fraudstream upgrade --install substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate \
  --version "$SUBSTRATE_VERSION" -n ate-system --wait --timeout 10m \
  --set rustfs.accessKey="$(rustfs accessKey)" --set rustfs.secretKey="$(rustfs secretKey)" >/dev/null

# After every upgrade: Helm puts the chart's command back.
echo "==> Valkey announcing its pod IP"
# shellcheck disable=SC2016
kubectl -n ate-system patch statefulset valkey-cluster --type json -p '[
  {"op": "replace", "path": "/spec/template/spec/containers/0/command",
   "value": ["valkey-server", "/etc/valkey/valkey.conf", "--cluster-announce-ip", "$(POD_IP)"]},
  {"op": "add", "path": "/spec/template/spec/containers/0/env",
   "value": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}]}]' >/dev/null
kubectl -n ate-system rollout status statefulset/valkey-cluster --timeout=5m
kubectl -n ate-system wait deploy --all --for=condition=Available --timeout=5m
