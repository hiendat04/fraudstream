#!/usr/bin/env bash
# Apply k8s/models/fraud-detection.yaml, wait for KServe and print the traffic split.
# The deploy stage and the rollout job both use it, so they apply the manifest the
# same way.
#
# The manifest names its model in the image tag, `fraudstream-serving:model-<version>`.
# The image deployed is `model-<version>-<code>`: the serving code of the last commit
# that changed it, plus that model version packaged inside from MLflow.
set -euo pipefail
cd "$(dirname "$0")/.."
MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:-http://mlflow:5000}
manifest=k8s/models/fraud-detection.yaml

# A manifest-only commit must not change the image, or every step of a rollout
# would add a revision. The code part of the tag follows the serving code alone.
SERVING_TAG=$(git log -1 --format=%h -- serving/src k8s/Dockerfile.serving k8s/Dockerfile.model)
version=$(sed -n 's#^ *image: .*fraudstream-serving:model-\([0-9][0-9]*\)$#\1#p' "$manifest")
[ -n "$version" ] || { echo "$manifest must name its model as fraudstream-serving:model-<version>" >&2; exit 1; }
code_image="dev.local/fraudstream-serving:$SERVING_TAG"
image="dev.local/fraudstream-serving:model-$version-$SERVING_TAG"

# A rollout step changes only the share, so the node usually has the image already.
if ! docker exec fraudstream-control-plane crictl inspecti "$image" >/dev/null 2>&1; then
  docker image inspect "$code_image" >/dev/null 2>&1 \
    || docker build -f k8s/Dockerfile.serving -t "$code_image" .
  packaged=$(mktemp -d)
  trap 'rm -rf "$packaged"' EXIT
  # Download through the MLflow server: its direct links name MinIO as minio:9000,
  # which only resolves inside the Compose network.
  (cd serving && MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false PYTHONPATH=src uv run python -m fraudstream_serving.fetch \
    "models:/fraud-detection/$version" "$packaged" --tracking-uri "$MLFLOW_TRACKING_URI")
  docker build -f k8s/Dockerfile.model --build-arg BASE="$code_image" -t "$image" "$packaged"
  kind load docker-image "$image" --name fraudstream
fi
sed "s#fraudstream-serving:model-$version\$#fraudstream-serving:model-$version-$SERVING_TAG#" "$manifest" | kubectl apply -f -

# Wait until KServe routes the manifest's share to the latest revision, then for Ready.
# Not --for=condition=Ready: that also needs KServe to record the new generation,
# which it skips when a change leaves its status as it was, so the wait never ends.
want=$(sed -n 's/^ *canaryTrafficPercent: *\([0-9]*\).*/\1/p' "$manifest")
want=${want:-100}
for i in $(seq 60); do
  got=$(kubectl -n kserve-models get inferenceservice fraud-detection \
    -o jsonpath='{.status.components.predictor.traffic[?(@.latestRevision==true)].percent}')
  [ "$got" = "$want" ] && break
  [ "$i" = 60 ] && { echo "the latest revision has ${got:-no}% of traffic after 5 minutes, the manifest says $want%" >&2; exit 1; }
  sleep 5
done
kubectl -n kserve-models wait --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
  inferenceservice/fraud-detection --timeout=5m
kubectl -n kserve-models get inferenceservice fraud-detection \
  -o jsonpath='{range .status.components.predictor.traffic[*]}{.revisionName} {.percent}% {.tag}{"\n"}{end}'

# With no canary, this version answers everything: point `production` at it, so the
# registry always names the model actually serving. Idempotent, safe to repeat.
if [ "$want" = 100 ]; then
  curl -sf -X POST "$MLFLOW_TRACKING_URI/api/2.0/mlflow/registered-models/alias" \
    -H 'Content-Type: application/json' \
    -d "{\"name\": \"fraud-detection\", \"alias\": \"production\", \"version\": \"$version\"}" >/dev/null
  echo "MLflow: fraud-detection@production -> version $version"
fi
