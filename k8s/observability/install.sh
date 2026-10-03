#!/usr/bin/env bash
# Install the observability stack on the local cluster. Everything lands in the
# observability namespace, and every UI sits behind the gateway. Safe to run again.
#   set -a && . ./.env && set +a && ./k8s/observability/install.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
. k8s/versions.sh
: "${GATEWAY_USER:?set GATEWAY_USER}" "${GATEWAY_PASSWORD:?set GATEWAY_PASSWORD}"
NS=observability

kubectl config use-context kind-fraudstream >/dev/null

echo "==> Namespace, gateway password, Grafana admin"
./k8s/gateway/credentials.sh "$NS"
kubectl -n "$NS" create secret generic grafana-admin \
  --from-literal=admin-user="$GATEWAY_USER" --from-literal=admin-password="$GATEWAY_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Prometheus, Grafana, node-exporter, kube-state-metrics"
helm upgrade --install metrics oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack \
  --version "$KUBE_PROMETHEUS_STACK_CHART" --namespace "$NS" \
  --values k8s/observability/prometheus-values.yaml --wait --timeout 10m

echo "==> Pushgateway"
helm upgrade --install pushgateway oci://ghcr.io/prometheus-community/charts/prometheus-pushgateway \
  --version "$PUSHGATEWAY_CHART" --namespace "$NS" \
  --values k8s/observability/pushgateway-values.yaml --wait --timeout 5m

echo "==> Jaeger"
sed "s/JAEGER_VERSION/$JAEGER_VERSION/" k8s/observability/jaeger.yaml | kubectl apply -f -
kubectl -n "$NS" rollout status deploy/jaeger --timeout=5m

echo "==> Elasticsearch and Kibana"
sed "s/ELASTIC_VERSION/$ELASTIC_VERSION/" k8s/observability/logging.yaml | kubectl apply -f -
kubectl -n "$NS" rollout status statefulset/elasticsearch --timeout=10m
kubectl -n "$NS" rollout status deploy/kibana --timeout=10m
kubectl -n "$NS" delete job logging-setup --ignore-not-found
kubectl apply -f k8s/observability/logging-setup.yaml
kubectl -n "$NS" wait --for=condition=complete job/logging-setup --timeout=10m

echo "==> Fluent Bit"
helm upgrade --install fluent-bit oci://ghcr.io/fluent/helm-charts/fluent-bit \
  --version "$FLUENT_BIT_CHART" --namespace "$NS" \
  --values k8s/observability/fluent-bit-values.yaml --wait --timeout 5m

echo "==> What Prometheus scrapes beyond the charts, and the dashboards"
kubectl apply -f k8s/observability/servicemonitors.yaml
kubectl apply -f k8s/observability/dashboards/

echo "==> Routes through the gateway"
kubectl apply -f k8s/observability/ingress.yaml

echo "==> Kubeflow Pipelines behind the gateway for the drift pipeline"
./k8s/gateway/credentials.sh kubeflow
kubectl apply -f k8s/gateway/pipelines.yaml

echo
echo "Grafana: https://grafana.fraudstream.localhost"
