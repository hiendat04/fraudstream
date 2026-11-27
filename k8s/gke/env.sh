# Sourced by the GKE scripts: settings from .env, the addresses Terraform created,
# the cluster settings k8s/cluster.sh reads, and the cluster's kubectl context,
# renamed fraudstream-gke, which also becomes the current one.

root="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../.." && pwd)"
set -a; . "$root/.env"; set +a

tf() { "$root/infra/terraform/tf.sh" "$@"; }
CLUSTER=$(tf gke output -raw cluster_name)
GATEWAY_IP=$(tf network output -raw gateway_ip)
REPO=$(tf registry output -raw repository_url)
VM_IP=$(tf platform-vm output -raw internal_ip 2>/dev/null || true)
VM_HOST=$(tf platform-vm output -raw external_ip 2>/dev/null || true)
# SSH to the platform VM as Ansible set it up:  ssh "${vm[@]}" <command>
vm=(-i ~/.ssh/fraudstream_gcp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o LogLevel=ERROR "fraudstream@$VM_HOST")
FRAUDSTREAM_CLUSTER=gke
FRAUDSTREAM_DOMAIN=$GATEWAY_IP.sslip.io
FRAUDSTREAM_IMAGE_REGISTRY=$REPO
CONTEXT=fraudstream-gke
export GATEWAY_IP VM_IP FRAUDSTREAM_CLUSTER FRAUDSTREAM_DOMAIN FRAUDSTREAM_IMAGE_REGISTRY

# get-credentials always writes gke_<project>_<zone>_<cluster>, so the old name goes first.
kubectl config delete-context "$CONTEXT" >/dev/null 2>&1 || true
gcloud container clusters get-credentials "$CLUSTER" --zone "$GCP_ZONE" --project "$GCP_PROJECT" >/dev/null
kubectl config rename-context "gke_${GCP_PROJECT}_${GCP_ZONE}_${CLUSTER}" "$CONTEXT" >/dev/null
kubectl config use-context "$CONTEXT" >/dev/null
