#!/usr/bin/env bash
# Installs External Secrets and connects the cluster to Vault: Vault's CA, the
# ServiceAccount Vault checks the cluster's tokens with, and Vault's Kubernetes
# auth pointed at this cluster. Each namespace's secrets.yaml then brings its own
# store and ExternalSecrets. Safe to run again, and after the cluster is made anew.
#   ./k8s/secrets/install.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
source k8s/versions.sh
kubectl() { command kubectl --context kind-fraudstream "$@"; }

echo "==> External Secrets ${ESO_VERSION}"
helm --kube-context kind-fraudstream upgrade --install external-secrets external-secrets \
  --repo https://charts.external-secrets.io --version "$ESO_VERSION" \
  -n external-secrets --create-namespace -f k8s/secrets/values.yaml --wait --timeout 5m >/dev/null

echo "==> Vault's CA and the token reviewer"
kubectl -n external-secrets create configmap vault-ca --from-file=ca.crt=infra/vault/tls/ca.crt \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f k8s/secrets/token-reviewer.yaml >/dev/null

./infra/vault/configure.sh kubernetes
