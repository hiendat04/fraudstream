# Sourced by the GKE scripts: settings from .env, the addresses Terraform
# created, and the cluster's kubectl context, which also becomes the current one.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
set -a; . "$root/.env"; set +a

tf() { "$root/infra/terraform/tf.sh" "$@"; }
CLUSTER=$(tf gke output -raw cluster_name)
GATEWAY_IP=$(tf gke output -raw gateway_ip)
REPO=$(tf registry output -raw repository_url)
STORES_DNS=$(tf stores-vm output -raw internal_dns 2>/dev/null || true)
CONTEXT="gke_${GCP_PROJECT}_${GCP_ZONE}_${CLUSTER}"

gcloud container clusters get-credentials "$CLUSTER" --zone "$GCP_ZONE" --project "$GCP_PROJECT" >/dev/null
kubectl config use-context "$CONTEXT" >/dev/null
