#!/usr/bin/env bash
# Start or end a cloud session. "up" builds the serving path on GCP. "down"
# removes everything that bills by the hour, then checks nothing was left behind.
# The network, registry, state bucket and budget stay.
#   ./infra/session.sh up | down | leftovers
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
tf=infra/terraform/tf.sh

preflight() {
  command -v gke-gcloud-auth-plugin >/dev/null \
    || { echo "gke-gcloud-auth-plugin is not on PATH" >&2; exit 1; }
  for name in postgres redis minio; do
    docker exec "fraudstream-$name" true >/dev/null 2>&1 \
      || { echo "fraudstream-$name is not running: the VM is seeded from the local stack" >&2; exit 1; }
  done
}

up() {
  preflight
  for stack in network registry gke stores-vm; do "$tf" "$stack" apply -auto-approve -input=false; done
  (cd infra/ansible && ansible-galaxy collection install -r requirements.yml >/dev/null && ansible-playbook site.yml)
  ./k8s/gke/platform.sh
  ./k8s/gke/deploy.sh
  kubectl config use-context kind-fraudstream >/dev/null 2>&1 || true
  gateway_ip=$("$tf" gke output -raw gateway_ip)
  echo "up: https://inference.$gateway_ip.sslip.io and https://drift-detection.$gateway_ip.sslip.io"
}

down() {
  if gcloud container clusters describe fraudstream --zone "$GCP_ZONE" --project "$GCP_PROJECT" >/dev/null 2>&1; then
    . k8s/gke/env.sh
    helm --kube-context "$CONTEXT" uninstall nginx-ingress -n nginx-ingress --wait 2>/dev/null || true
    for _ in $(seq 60); do
      [ -z "$(gcloud compute forwarding-rules list --project "$GCP_PROJECT" \
        --filter="IPAddress=$GATEWAY_IP" --format='value(name)')" ] && break
      sleep 5
    done
  fi
  "$tf" stores-vm destroy -auto-approve -input=false
  "$tf" gke destroy -auto-approve -input=false
  kubectl config delete-context "gke_${GCP_PROJECT}_${GCP_ZONE}_fraudstream" >/dev/null 2>&1 || true
  kubectl config use-context kind-fraudstream >/dev/null 2>&1 || true
  leftovers
}

leftovers() {
  found=0
  for kind in "compute instances" "compute disks" "compute forwarding-rules" "compute target-pools" \
              "compute addresses" "container clusters"; do
    items=$(gcloud $kind list --project "$GCP_PROJECT" --format='value(name)' 2>/dev/null) \
      || { echo "could not list $kind" >&2; exit 1; }
    if [ -n "$items" ]; then echo "  LEFT: $kind: $(echo "$items" | paste -sd ' ' -)"; found=1; else echo "  none: $kind"; fi
  done
  if [ "$found" = 0 ]; then
    echo "nothing billing by the hour"
  else
    echo "something is still billing" >&2
    exit 1
  fi
}

case "${1:-}" in
  up | down | leftovers) "$1" ;;
  *) echo "usage: $0 up | down | leftovers" >&2; exit 2 ;;
esac
