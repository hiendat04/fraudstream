#!/usr/bin/env bash
# Apply k8s/models/fraud-detection.yaml with the image of the last serving code
# change, wait for KServe and print the traffic split. The deploy stage and the
# rollout job both use it, so they apply the manifest the same way.
set -euo pipefail
cd "$(dirname "$0")/.."

# A manifest-only commit must not change the image, or every step of a rollout
# would add a revision. The tag follows the serving code alone.
SERVING_TAG=$(git log -1 --format=%h -- serving/src k8s/Dockerfile.serving)
image="dev.local/fraudstream-serving:$SERVING_TAG"
# A rollout step changes only the manifest, so the node usually has the image already.
if ! docker exec fraudstream-control-plane crictl inspecti "$image" >/dev/null 2>&1; then
  docker build -f k8s/Dockerfile.serving -t "$image" .
  kind load docker-image "$image" --name fraudstream
fi
sed "s#fraudstream-serving:dev#fraudstream-serving:$SERVING_TAG#" k8s/models/fraud-detection.yaml | kubectl apply -f -
kubectl -n kserve-models wait --for=condition=Ready inferenceservice/fraud-detection --timeout=5m
kubectl -n kserve-models get inferenceservice fraud-detection \
  -o jsonpath='{range .status.components.predictor.traffic[*]}{.revisionName} {.percent}% {.tag}{"\n"}{end}'
