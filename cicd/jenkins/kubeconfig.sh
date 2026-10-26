#!/usr/bin/env bash
# Copy the host kubeconfig into Jenkins and point it at the kind node.
set -euo pipefail

CONTAINER=${1:-fraudstream-jenkins}
docker exec "$CONTAINER" mkdir -p /var/jenkins_home/.kube
docker cp "${KUBECONFIG:-$HOME/.kube/config}" "$CONTAINER:/var/jenkins_home/.kube/config"
docker exec "$CONTAINER" kubectl --kubeconfig /var/jenkins_home/.kube/config \
  config set-cluster kind-fraudstream --server=https://fraudstream-control-plane:6443
docker exec "$CONTAINER" kubectl --kubeconfig /var/jenkins_home/.kube/config get nodes
