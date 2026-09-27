#!/usr/bin/env bash
# Install the gateway and the autoscaler on the GKE cluster: NGINX Ingress on the
# reserved IP, cert-manager with the local CA, and KEDA. GKE brings its own
# metrics-server.
#   ./k8s/gke/platform.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
. k8s/gke/env.sh
. k8s/versions.sh

helm --kube-context "$CONTEXT" upgrade --install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version "$NGINX_INGRESS_CHART" --namespace nginx-ingress --create-namespace \
  --values k8s/gke/nginx-ingress-values.yaml \
  --set controller.service.loadBalancerIP="$GATEWAY_IP" --wait --timeout 10m

helm --kube-context "$CONTEXT" upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version "$CERT_MANAGER_VERSION" --namespace cert-manager --create-namespace \
  --values k8s/platform/cert-manager-values.yaml --wait --timeout 5m
kubectl --context "$CONTEXT" -n cert-manager wait --for=condition=Available --timeout=5m deploy/cert-manager-webhook
kubectl --context "$CONTEXT" apply -f k8s/platform/local-ca-issuer.yaml
kubectl --context "$CONTEXT" -n cert-manager wait --for=condition=Ready --timeout=2m certificate/local-ca

kubectl --context "$CONTEXT" apply --server-side \
  -f "https://github.com/kedacore/keda/releases/download/${KEDA_VERSION}/keda-${KEDA_VERSION#v}.yaml"
kubectl --context "$CONTEXT" -n keda wait --for=condition=Available --timeout=5m deploy --all

echo "gateway on $GATEWAY_IP"
