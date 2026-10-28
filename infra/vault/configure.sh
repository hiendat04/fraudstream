#!/usr/bin/env bash
# Sets who may log in to Vault and what each may read: one policy per reader from
# infra/vault/policies/, an AppRole each for Jenkins and Airflow, and Kubernetes
# auth with one role per namespace, for that namespace's ServiceAccount
# vault-secrets. Uses the root token in ~/.fraudstream/vault-init.json. Safe to run
# again; run it with `kubernetes` alone after the kind cluster is made anew.
#   ./infra/vault/configure.sh
#   ./infra/vault/configure.sh kubernetes
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/infra/vault/tls/ca.crt}"
VAULT_TOKEN=$(jq -r .root_token ~/.fraudstream/vault-init.json)
export VAULT_ADDR VAULT_CACERT VAULT_TOKEN
kubectl() { command kubectl --context kind-fraudstream "$@"; }

# A JSON body from stdin and the token from a file descriptor, so a secret in the
# body never shows in a command line.
api() {
  curl -sf --cacert "$VAULT_CACERT" -H @<(printf 'X-Vault-Token: %s\n' "$VAULT_TOKEN") \
    -X POST --data @- "$VAULT_ADDR/v1/$1" >/dev/null
}
enable_auth() { vault auth list -format=json | jq -e --arg m "$1/" '.[$m]' >/dev/null || vault auth enable "$1" >/dev/null; }

policies() {
  echo "==> policies"
  local file
  for file in infra/vault/policies/*.hcl; do
    vault policy write "$(basename "$file" .hcl)" "$file" >/dev/null
    echo "  $(basename "$file" .hcl)"
  done
}

approles() {
  echo "==> AppRoles for Jenkins and Airflow"
  enable_auth approle
  local role
  for role in jenkins airflow; do
    vault write "auth/approle/role/$role" token_policies="$role" \
      token_ttl=1h token_max_ttl=4h secret_id_ttl=0 >/dev/null
    echo "  $role"
  done
}

kubernetes() {
  echo "==> Kubernetes auth, one role per namespace"
  enable_auth kubernetes
  local ca reviewer namespace
  ca=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)
  reviewer=$(kubectl -n external-secrets get secret vault-token-reviewer -o jsonpath='{.data.token}' | base64 -d)
  # Vault reaches the API server over the Compose network the kind node joined.
  jq -n --arg ca "$ca" --arg jwt "$reviewer" \
    '{kubernetes_host: "https://fraudstream-control-plane:6443", kubernetes_ca_cert: $ca, token_reviewer_jwt: $jwt}' \
    | api auth/kubernetes/config
  for namespace in $(basename -s .hcl infra/vault/policies/k8s-*.hcl | sed 's/^k8s-//'); do
    vault write "auth/kubernetes/role/$namespace" bound_service_account_names=vault-secrets \
      bound_service_account_namespaces="$namespace" token_policies="k8s-$namespace" token_ttl=15m >/dev/null
    echo "  $namespace"
  done
}

case "${1:-all}" in
  all) policies; approles; kubernetes ;;
  kubernetes) kubernetes ;;
  *) echo "usage: configure.sh [kubernetes]" >&2; exit 2 ;;
esac
