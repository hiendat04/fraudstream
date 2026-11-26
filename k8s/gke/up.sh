#!/usr/bin/env bash
# Install the whole Kubernetes platform on GKE with the same installers kind uses.
# Needs the platform VM up (Ansible done) and `vault login -method=userpass
# username=operator`. Every Vault call goes to the VM's Vault, through an SSH tunnel:
# that's the Vault the cluster's External Secrets read. Safe to run again.
#   ./k8s/gke/up.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
. k8s/gke/env.sh
: "${VM_IP:?the platform VM is not up: apply the platform-vm stack and run Ansible first}"
TAG=$(git rev-parse --short=7 HEAD)
vm=(-i ~/.ssh/fraudstream_gcp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o LogLevel=ERROR "fraudstream@$VM_HOST")

echo "==> Tunnel to the VM's Vault"
tunnel=$(mktemp -u)
ssh -f -N -M -S "$tunnel" -o ExitOnForwardFailure=yes -L "18200:$VM_IP:8200" "${vm[@]}"
trap 'ssh -S "$tunnel" -O exit "${vm[@]}" 2>/dev/null' EXIT
# The certificate names localhost, so the tunnel's end verifies like the local's Vault.
export VAULT_ADDR=https://localhost:18200 VAULT_CACERT=$PWD/infra/vault/tls/ca.crt
# The VM's Vault knows only the tokens in its snapshot. A login made since is asked for
# again here; the token stays in this shell and the local ~/.vault-token is left alone.
if ! vault token lookup >/dev/null 2>&1; then
  echo "    the VM's Vault doesn't know your token: log in to it"
  VAULT_TOKEN=$(vault login -token-only -method=userpass username=operator)
  export VAULT_TOKEN
fi

echo "==> Namespaces, and the VM's stores under the names pods use"
services=$(k8s/gke/vm_services.sh "$VM_IP")
for namespace in $(grep -v '^---$' <<<"$services" | jq -r .metadata.namespace | sort -u); do
  kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done
kubectl apply -f - <<<"$services" >/dev/null

./k8s/bootstrap.sh
./k8s/observability/install.sh
./k8s/llm/install.sh

echo "==> MCP image fraudstream-mcp:$TAG, built on the VM for the cluster's amd64 nodes"
git archive HEAD | ssh "${vm[@]}" "docker build -q -f k8s/Dockerfile.mcp -t fraudstream-mcp:$TAG - >/dev/null \
  && cd /opt/fraudstream && set -a && . ./.env && set +a && ./cicd/ship_image.sh fraudstream-mcp:$TAG"
TAG=$TAG IMAGE_PUSHED=1 ./k8s/mcp/install.sh

./k8s/agents/install.sh
echo "up: the platform on $CONTEXT, the gateway on https://*.$FRAUDSTREAM_DOMAIN"
