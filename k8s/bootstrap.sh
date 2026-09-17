#!/usr/bin/env bash
#
# Creates the local cluster the training pipeline runs on, and installs the two
# pieces it needs: Kubeflow Pipelines to orchestrate, and Kubeflow Trainer to
# run the distributed training step. Both install on their own -- neither drags
# in the rest of the Kubeflow platform.
#
# Run it with:   ./k8s/bootstrap.sh
# Start over with: kind delete cluster --name fraudstream
#
# The steps below run in order and stop at the first failure.

set -euo pipefail

CLUSTER_NAME=fraudstream
PIPELINE_VERSION=2.17.0
TRAINER_VERSION=v2.2.0
NGINX_INGRESS_CHART=2.7.3
CERT_MANAGER_VERSION=v1.21.2
METRICS_SERVER_CHART=3.14.0
COMPOSE_NETWORK=fraudstream_default
HERE="$(cd "$(dirname "$0")" && pwd)"


echo "==> Checking the tools are installed and runnable"
# Running each one also catches a binary built for the wrong platform, which
# sits on PATH looking perfectly fine until something tries to execute it.
if ! kind version >/dev/null 2>&1; then
  echo "Error: kind will not run. Check:  file \$(command -v kind)"
  exit 1
fi
if ! kubectl version --client >/dev/null 2>&1; then
  echo "Error: kubectl will not run. Check:  file \$(command -v kubectl)"
  exit 1
fi
if ! helm version >/dev/null 2>&1; then
  echo "Error: helm will not run. Check:  file \$(command -v helm)"
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "Error: docker is not running or will not work."
  exit 1
fi


echo "==> Creating the cluster"
if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --config "$HERE/kind-cluster.yaml"
fi
kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null


echo "==> Connecting the cluster to the lakehouse network"
# kind puts its node on a Docker network of its own, so until it joins the
# compose network the pods cannot resolve postgres or minio at all.
# The error when it is already connected is not interesting, so ignore it.
docker network connect "$COMPOSE_NETWORK" "${CLUSTER_NAME}-control-plane" 2>/dev/null || true


echo "==> Installing Kubeflow Pipelines ${PIPELINE_VERSION}"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${PIPELINE_VERSION}"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=${PIPELINE_VERSION}"


echo "==> Installing Kubeflow Trainer ${TRAINER_VERSION}"
# The CRDs go on separately because helm only ever installs them once and never
# updates them afterwards. The sed is there because `helm show crds` prints its
# download progress onto stdout, which kubectl then tries to read as YAML.
helm show crds oci://ghcr.io/kubeflow/charts/kubeflow-trainer --version "${TRAINER_VERSION#v}" 2>/dev/null \
  | sed -n '/^---$/,$p' \
  | kubectl apply --server-side --force-conflicts -f - >/dev/null

# Skipped once it is installed. Running helm again over a live install fails:
# the controller writes its own webhook certificates at startup and helm then
# argues with it over who owns them. To change TRAINER_VERSION, uninstall first:
#   helm uninstall kubeflow-trainer -n kubeflow-system
if ! helm status kubeflow-trainer --namespace kubeflow-system >/dev/null 2>&1; then
  helm install kubeflow-trainer oci://ghcr.io/kubeflow/charts/kubeflow-trainer \
    --namespace kubeflow-system --create-namespace \
    --version "${TRAINER_VERSION#v}" --wait --timeout 10m
fi


echo "==> Installing the training runtimes"
# These are checked by a webhook the controller serves, so the controller has to
# be up before they can be created. They are applied with kubectl rather than
# through helm to avoid the certificate argument described above.
kubectl -n kubeflow-system wait --for=condition=Available --timeout=5m \
  deploy/kubeflow-trainer-controller-manager
kubectl apply --server-side -k \
  "https://github.com/kubeflow/trainer.git/manifests/overlays/runtimes?ref=${TRAINER_VERSION}"


echo "==> Loading the lakehouse credentials into the cluster"
# Only the username and password pairs. Host names and ports are already set in
# the training image with values that work inside the cluster, and .env holds
# the localhost versions, so copying the whole file in here would break them.
set -a; . "${HERE}/../.env"; set +a
kubectl create secret generic lakehouse-credentials --namespace kubeflow \
  --from-literal=POSTGRES_USER="$POSTGRES_USER" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  --from-literal=MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -


echo "==> Switching off the two components with no build for this machine"
# Kubeflow publishes these for Intel machines only, so on an Apple Silicon one
# they can never start. Neither is needed here: proxy-agent exists to expose the
# UI on Google Cloud, and pipelines record their metadata through metadata-grpc
# rather than through metadata-writer.
kubectl -n kubeflow scale deploy metadata-writer proxy-agent --replicas=0


echo "==> Installing the NGINX Ingress Controller ${NGINX_INGRESS_CHART}"
# This is F5's controller. The older ingress-nginx project was retired in March
# 2026. It listens on the node's ports 80 and 443, which kind maps to the local network.
helm upgrade --install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version "${NGINX_INGRESS_CHART}" \
  --namespace nginx-ingress --create-namespace \
  --values "$HERE/platform/nginx-ingress-values.yaml" \
  --wait --timeout 5m


echo "==> Installing cert-manager ${CERT_MANAGER_VERSION}"
# cert-manager makes the certificates that KServe's webhook needs, and the HTTPS
# certificates for the ingress.
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version "${CERT_MANAGER_VERSION}" \
  --namespace cert-manager --create-namespace \
  --values "$HERE/platform/cert-manager-values.yaml" \
  --wait --timeout 5m


echo "==> Creating the local certificate authority"
# cert-manager checks new issuers through its webhook, so the webhook must be up
# first. local-ca signs the HTTPS certificates for *.localhost names.
kubectl -n cert-manager wait --for=condition=Available --timeout=5m deploy/cert-manager-webhook
kubectl apply -f "$HERE/platform/local-ca-issuer.yaml"
kubectl -n cert-manager wait --for=condition=Ready --timeout=2m certificate/local-ca


echo "==> Installing metrics-server ${METRICS_SERVER_CHART}"
# kind uses self-signed kubelet certificates that Metrics Server cannot verify.
# --kubelet-insecure-tls lets Metrics Server connect to the kubelet anyway.
# We download the Helm chart directly from GitHub instead of using a Helm repo.
# This avoids problems with other Helm repos configured on the machine.
CHART_DIR="$(mktemp -d)"
curl -sfL -o "$CHART_DIR/metrics-server.tgz" \
  "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-${METRICS_SERVER_CHART}/metrics-server-${METRICS_SERVER_CHART}.tgz"
helm upgrade --install metrics-server "$CHART_DIR/metrics-server.tgz" \
  --namespace kube-system \
  --values "$HERE/platform/metrics-server-values.yaml" \
  --wait --timeout 5m
rm -rf "$CHART_DIR"


echo "==> Waiting for everything to come up"
kubectl -n kubeflow-system wait --for=condition=Available --timeout=10m deploy --all
kubectl -n kubeflow wait --for=condition=Available --timeout=20m deploy --all
kubectl -n nginx-ingress wait --for=condition=Available --timeout=5m deploy --all
kubectl -n cert-manager wait --for=condition=Available --timeout=5m deploy --all
kubectl -n kube-system wait --for=condition=Available --timeout=5m deploy/metrics-server


echo "==> Checking the XGBoost runtime exists"
kubectl get clustertrainingruntime xgboost-distributed


echo "==> Checking the pods can reach the lakehouse"
kubectl run lakehouse-probe --rm -i --restart=Never --image=busybox:1.36 --command -- \
  sh -c 'nc -z -w3 postgres 5432 && echo "postgres:5432 reachable"; nc -z -w3 minio 9000 && echo "minio:9000 reachable"'


echo
echo "Done."
echo "  Open the Pipelines UI:  kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8080:80"
echo "  Delete the cluster:     kind delete cluster --name ${CLUSTER_NAME}"
