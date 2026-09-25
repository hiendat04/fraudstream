#!/usr/bin/env bash
# Store the gateway's username and password as the htpasswd Secret NGINX reads
# for basic auth. Run it for every namespace with a service behind the gateway.
#   set -a && . ./.env && set +a && ./k8s/gateway/credentials.sh fraudstream-apis
set -euo pipefail

namespace=$1
: "${GATEWAY_USER:?set GATEWAY_USER}" "${GATEWAY_PASSWORD:?set GATEWAY_PASSWORD}"

hash=$(printf '%s' "$GATEWAY_PASSWORD" | openssl passwd -apr1 -stdin)
kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$namespace" create secret generic gateway-htpasswd --type=nginx.org/htpasswd \
  --from-literal=htpasswd="$GATEWAY_USER:$hash" \
  --dry-run=client -o yaml | kubectl apply -f -
