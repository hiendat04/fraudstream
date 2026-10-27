#!/usr/bin/env bash
# Installs Agent Substrate on the kind cluster, the runtime that runs sandboxed
# agents as gVisor actors on a pool of pre-warmed workers: its CRDs, then its
# control plane, router, node agent, Valkey and RustFS in ate-system. RustFS,
# which keeps the actors' snapshots, gets a key pair made once and kept, in
# place of the chart's default login. Valkey on the 0.0.9 chart remembers pod
# IPs, so after its pods move (a Docker restart) the cluster stays down: each
# node is made to announce its current IP, the nodes are told where the others
# are now, and the API server and controller reconnect. kagent's chart makes the
# worker pool. Safe to run again, and run it again after a Docker restart.
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

# No --wait: the API server can't be ready until Valkey is fixed below.
echo "==> Agent Substrate ${SUBSTRATE_VERSION}"
helm --kube-context kind-fraudstream upgrade --install substrate-crds oci://ghcr.io/kagent-dev/substrate/helm/substrate-crds \
  --version "$SUBSTRATE_VERSION" -n ate-system --wait >/dev/null
helm --kube-context kind-fraudstream upgrade --install substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate \
  --version "$SUBSTRATE_VERSION" -n ate-system --force-conflicts \
  --set rustfs.accessKey="$(rustfs accessKey)" --set rustfs.secretKey="$(rustfs secretKey)" >/dev/null

echo "==> Valkey"
# shellcheck disable=SC2016
kubectl -n ate-system patch statefulset valkey-cluster --type json -p '[
  {"op": "replace", "path": "/spec/template/spec/containers/0/command",
   "value": ["valkey-server", "/etc/valkey/valkey.conf", "--cluster-announce-ip", "$(POD_IP)"]},
  {"op": "add", "path": "/spec/template/spec/containers/0/env",
   "value": [{"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}]}]' >/dev/null
kubectl -n ate-system rollout status statefulset/valkey-cluster --timeout=5m
# After the chart's job made the cluster: meeting nodes before that would stop it.
kubectl -n ate-system wait job/valkey-cluster-init --for=condition=Complete --timeout=5m >/dev/null
for ip in $(kubectl -n ate-system get pods -l app=valkey-cluster -o jsonpath='{.items[*].status.podIP}'); do
  kubectl -n ate-system exec valkey-cluster-0 -c valkey -- valkey-cli cluster meet "$ip" 6379 >/dev/null
done

echo "==> the API server and the controller, on Valkey's current IPs"
kubectl -n ate-system rollout restart deploy/ate-api-server-deployment deploy/ate-controller >/dev/null
# Ready only once it reaches Valkey, so this is the check that the cluster is back.
kubectl -n ate-system rollout status deploy/ate-api-server-deployment --timeout=5m
kubectl -n ate-system wait deploy --all --for=condition=Available --timeout=5m
