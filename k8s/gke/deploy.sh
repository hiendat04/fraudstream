#!/usr/bin/env bash
# Build the serving images for GKE's amd64 machines, push them to the registry,
# and deploy the model and the two APIs behind the gateway. The cluster can't reach
# Vault, so its Secrets are copied from Vault here, at deploy time.
# Needs Vault unsealed and `vault login -method=userpass username=operator`.
#   ./k8s/gke/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${VAULT_ADDR:=https://localhost:8200}" "${VAULT_CACERT:=$PWD/infra/vault/tls/ca.crt}"
export VAULT_ADDR VAULT_CACERT
vault_field() { vault kv get -mount=secret -field="$2" "$1"; }
# Read before anything is built, so a sealed Vault stops the deploy here; the values
# reach kubectl as files, never as arguments.
postgres_password=$(vault_field cloud/postgres password)
htpasswd=$(vault_field platform/gateway-htpasswd htpasswd)
GATEWAY_USER=$(vault_field platform/gateway login)
GATEWAY_PASSWORD=$(vault_field platform/gateway password)
export GATEWAY_USER GATEWAY_PASSWORD
. k8s/gke/env.sh
TAG=$(git rev-parse --short=7 HEAD)
NS=fraudstream-apis
: "${STORES_DNS:?the stores VM is not up: apply the stores-vm stack and run Ansible first}"

gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev" --quiet >/dev/null
for name in inference drift-detection serving; do
  docker buildx build --platform linux/amd64 -f "k8s/Dockerfile.$name" \
    -t "$REPO/fraudstream-$name:$TAG" --push .
done

kubectl --context "$CONTEXT" create namespace "$NS" --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -
sed "s#STORES_HOST#$STORES_DNS#" k8s/gke/stores.yaml | kubectl --context "$CONTEXT" apply -f -
kubectl --context "$CONTEXT" -n "$NS" create secret generic feature-store-registry \
  --from-literal=POSTGRES_USER=fraudstream --from-file=POSTGRES_PASSWORD=<(printf %s "$postgres_password") \
  --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -
kubectl --context "$CONTEXT" -n "$NS" create secret generic gateway-htpasswd --type=nginx.org/htpasswd \
  --from-file=htpasswd=<(printf %s "$htpasswd") --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -

sed "s#SERVING_IMAGE#$REPO/fraudstream-serving:$TAG#" k8s/gke/fraud-detection.yaml | kubectl --context "$CONTEXT" apply -f -
kubectl --context "$CONTEXT" -n "$NS" rollout status deploy/fraud-detection --timeout=10m

api() {
  local release=$1 image=$2 host=$3.$GATEWAY_IP.sslip.io
  shift 3
  helm --kube-context "$CONTEXT" upgrade --install "$release" k8s/charts/fraudstream-api -n "$NS" \
    -f "k8s/apis/$release.yaml" --set image.repository="$REPO/$image" --set-string image.tag="$TAG" \
    --set ingress.host="$host" --set-string env.OTEL_EXPORTER_OTLP_ENDPOINT= "$@" --rollback-on-failure --timeout 5m
  GATEWAY_ADDRESS="$GATEWAY_IP" ./cicd/wait_for_version.sh "$host" "$TAG"
  GATEWAY_ADDRESS="$GATEWAY_IP" ./cicd/check_gateway.sh "$host"
}

api drift-detection fraudstream-drift-detection drift-detection
api inference-api fraudstream-inference inference \
  --set env.MODEL_URL="http://fraud-detection.$NS.svc.cluster.local/v1/models/fraud-detection:predict"
