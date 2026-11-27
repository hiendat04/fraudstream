#!/usr/bin/env bash
# Give Jenkins a kubeconfig for the cluster it deploys to.
#   ./cicd/jenkins/kubeconfig.sh                          kind: the host's kubeconfig, pointed at the kind node
#   FRAUDSTREAM_CLUSTER=gke ./cicd/jenkins/kubeconfig.sh  GKE: the jenkins-deployer account's token,
#                                                          written into the platform VM's Jenkins from the local
set -euo pipefail
cd "$(dirname "$0")/../.."

CONTAINER=${1:-fraudstream-jenkins}
CONFIG=/var/jenkins_home/.kube/config

if [ "${FRAUDSTREAM_CLUSTER:-kind}" = kind ]; then
  docker exec "$CONTAINER" mkdir -p /var/jenkins_home/.kube
  docker cp "${KUBECONFIG:-$HOME/.kube/config}" "$CONTAINER:$CONFIG"
  docker exec "$CONTAINER" kubectl --kubeconfig "$CONFIG" \
    config set-cluster kind-fraudstream --server=https://fraudstream-control-plane:6443
  docker exec "$CONTAINER" kubectl --kubeconfig "$CONFIG" get nodes
  exit 0
fi

. k8s/gke/env.sh
kubectl apply -f k8s/rbac/jenkins-deployer.yaml >/dev/null
stored_token() { kubectl -n kube-system get secret jenkins-deployer -o jsonpath='{.data.token}'; }
# Kubernetes writes the token into the Secret a moment after the Secret is made.
for _ in $(seq 30); do [ -n "$(stored_token)" ] && break; sleep 1; done
token=$(stored_token | base64 -d)
: "${token:?the jenkins-deployer Secret has no token after 30 seconds}"
server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
ca=$(kubectl -n kube-system get secret jenkins-deployer -o jsonpath='{.data.ca\.crt}')

# The token travels on SSH's stdin into the file
ssh "${vm[@]}" "docker exec -i $CONTAINER sh -c 'umask 077 && mkdir -p ${CONFIG%/*} && cat > $CONFIG'" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: $CONTEXT
    cluster:
      server: $server
      certificate-authority-data: $ca
users:
  - name: jenkins-deployer
    user:
      token: $token
contexts:
  - name: $CONTEXT
    context: {cluster: $CONTEXT, user: jenkins-deployer}
current-context: $CONTEXT
EOF
ssh "${vm[@]}" docker exec "$CONTAINER" kubectl get nodes
