#!/usr/bin/env bash
#
# Creates the local cluster the training pipeline runs on, and installs the two
# control planes it needs: Kubeflow Pipelines for orchestration and Kubeflow
# Trainer for distributed training. Both install standalone -- neither pulls in
# the rest of the Kubeflow platform.
#
# Safe to re-run; every step checks before acting.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-fraudstream}"
PIPELINE_VERSION="${PIPELINE_VERSION:-2.17.0}"
TRAINER_VERSION="${TRAINER_VERSION:-v2.2.0}"
COMPOSE_NETWORK="${COMPOSE_NETWORK:-fraudstream_default}"

CONTEXT="kind-${CLUSTER_NAME}"
NODE_CONTAINER="${CLUSTER_NAME}-control-plane"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
warn() { printf '\033[33m    warning: %s\033[0m\n' "$1"; }
die()  { printf '\033[31m    error: %s\033[0m\n' "$1" >&2; exit 1; }

# --------------------------------------------------------------------------
log "Checking required tools"
for tool in kind kubectl helm docker; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
  # Run it rather than just find it. A binary built for another platform sits
  # on PATH quite happily and only fails once this script is already midway
  # through creating things.
  case "$tool" in
    kubectl) probe=(kubectl version --client) ;;
    docker)  probe=(docker --version) ;;
    *)       probe=("$tool" version) ;;
  esac
  "${probe[@]}" >/dev/null 2>&1 \
    || die "$tool at $(command -v "$tool") will not run here -- wrong platform build? try 'file \$(command -v $tool)'"
  printf '    %-8s %s\n' "$tool" "$(command -v "$tool")"
done
docker info >/dev/null 2>&1 || die "docker is not running"

# --------------------------------------------------------------------------
log "Creating the cluster"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "    cluster '$CLUSTER_NAME' already exists, reusing it"
else
  kind create cluster --config "$HERE/kind-cluster.yaml"
fi
kubectl config use-context "$CONTEXT" >/dev/null

server_version="$(kubectl version -o json | python3 -c \
  'import sys,json,re; v=json.load(sys.stdin)["serverVersion"]; print(re.sub(r"\D","",v["major"])+"."+re.sub(r"\D","",v["minor"]))')"
echo "    kubernetes ${server_version}"
python3 -c "import sys; major,minor=map(int,'${server_version}'.split('.')); sys.exit(0 if (major,minor) >= (1,31) else 1)" \
  || die "Kubeflow Trainer requires Kubernetes >= 1.31, cluster is ${server_version}"

# --------------------------------------------------------------------------
log "Attaching the cluster to the lakehouse network"
# kind runs its node as a Docker container on its own network, so without this
# the pipeline pods cannot resolve the compose services at all.
if ! docker network inspect "$COMPOSE_NETWORK" >/dev/null 2>&1; then
  warn "network '$COMPOSE_NETWORK' not found -- start the lakehouse with 'docker compose up -d postgres minio redis'"
elif docker inspect "$NODE_CONTAINER" \
      --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | grep -qw "$COMPOSE_NETWORK"; then
  echo "    already attached to '$COMPOSE_NETWORK'"
else
  docker network connect "$COMPOSE_NETWORK" "$NODE_CONTAINER"
  echo "    attached to '$COMPOSE_NETWORK'"
fi

# --------------------------------------------------------------------------
log "Installing Kubeflow Pipelines ${PIPELINE_VERSION}"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${PIPELINE_VERSION}"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=${PIPELINE_VERSION}"

# --------------------------------------------------------------------------
log "Installing Kubeflow Trainer ${TRAINER_VERSION}"
# Helm installs CRDs on first install and never upgrades them, so a version bump
# leaves the old schema in place and the new runtime fails to apply against it.
# `helm show crds` prints its OCI "Pulled:"/"Digest:" progress to stdout, which
# kubectl reads as a malformed first document, so start at the first separator.
helm show crds oci://ghcr.io/kubeflow/charts/kubeflow-trainer --version "${TRAINER_VERSION#v}" 2>/dev/null \
  | sed -n '/^---$/,$p' \
  | kubectl apply --server-side --force-conflicts -f - >/dev/null
echo "    CRDs applied at ${TRAINER_VERSION}"

# Re-running helm over a live install fails: the controllers mint their own
# webhook certificates at startup, and helm then fights them for ownership of
# those secrets. Nothing needs re-applying when the target version is already
# deployed, so skip it. Changing TRAINER_VERSION needs `helm uninstall
# kubeflow-trainer -n kubeflow-system` first, or a fresh cluster.
installed_version="$(helm list -n kubeflow-system -o json 2>/dev/null | python3 -c "
import sys, json
try: releases = json.load(sys.stdin)
except Exception: releases = []
for r in releases:
    if r.get('name') == 'kubeflow-trainer':
        print(r['chart'].rsplit('-', 1)[-1]); break
" 2>/dev/null || true)"

if [ "$installed_version" = "${TRAINER_VERSION#v}" ] \
   && kubectl -n kubeflow-system get deploy kubeflow-trainer-controller-manager >/dev/null 2>&1; then
  echo "    ${TRAINER_VERSION} controller already present, leaving it alone"
else
  # Runtimes stay switched off here on purpose. The controller serves a
  # validating webhook for ClusterTrainingRuntime, so it has to be running
  # before any runtime is created; enabling them in this same release races
  # that webhook and fails with "connection refused".
  helm upgrade --install kubeflow-trainer oci://ghcr.io/kubeflow/charts/kubeflow-trainer \
    --namespace kubeflow-system \
    --create-namespace \
    --version "${TRAINER_VERSION#v}" \
    --wait --timeout 10m
fi

log "Installing the training runtimes"
# Applied with kubectl rather than a second `helm upgrade`: the controllers
# generate their own webhook certificates at startup, and Helm's apply then
# fights them for ownership of those secrets. The overlay carries every runtime
# -- the ones this project does not use are inert templates that start no pods.
for attempt in 1 2 3 4 5 6; do
  if kubectl apply --server-side -k \
      "https://github.com/kubeflow/trainer.git/manifests/overlays/runtimes?ref=${TRAINER_VERSION}" >/dev/null 2>&1; then
    echo "    runtimes applied"
    break
  fi
  [ "$attempt" = 6 ] && die "runtimes would not apply -- is the trainer webhook serving?"
  echo "    webhook not ready yet, retrying (${attempt}/6)"
  sleep 10
done

# --------------------------------------------------------------------------
log "Switching off components that have no build for this machine"
# Kubeflow publishes these two images for linux/amd64 only, so on an arm64 host
# they can never start. Neither is needed to run pipelines locally: proxy-agent
# is the Google Cloud inverting proxy (we reach the UI by port-forward), and
# metadata-writer is the v1 lineage sync, while v2 pipelines write metadata
# through metadata-grpc instead. Scaled to zero so the cluster reports its real
# state rather than sitting in a permanent ImagePullBackOff.
for deploy in metadata-writer proxy-agent; do
  if kubectl -n kubeflow get deploy "$deploy" >/dev/null 2>&1; then
    kubectl -n kubeflow scale deploy "$deploy" --replicas=0 >/dev/null
    echo "    $deploy scaled to 0"
  fi
done

log "Waiting for both control planes to come up"
kubectl -n kubeflow-system wait --for=condition=Available --timeout=10m deploy --all
kubectl -n kubeflow wait --for=condition=Available --timeout=20m deploy --all

# --------------------------------------------------------------------------
log "Verifying the XGBoost training runtime is installed"
kubectl get clustertrainingruntimes
kubectl get clustertrainingruntime xgboost-distributed >/dev/null 2>&1 \
  || die "'xgboost-distributed' runtime is missing -- the distributed training step cannot run without it"
echo "    xgboost-distributed is available"

# --------------------------------------------------------------------------
log "Checking that pods can reach the lakehouse"
kubectl run lakehouse-probe --rm -i --restart=Never --image=busybox:1.36 --command -- \
  sh -c 'nc -z -w3 postgres 5432 && echo "    postgres:5432 reachable"; nc -z -w3 minio 9000 && echo "    minio:9000 reachable"' \
  || warn "lakehouse probe failed -- pipeline components will not be able to read features"

# --------------------------------------------------------------------------
log "Done"
cat <<EOF
    Open the Pipelines UI with:
      kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8080:80
      open http://localhost:8080

    Tear the cluster down with:
      kind delete cluster --name ${CLUSTER_NAME}
EOF
