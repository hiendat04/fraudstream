#!/usr/bin/env bash
# Run Terraform for one stack of the dev environment, with the settings from .env
# and the stack's state in the project's bucket.
#   ./infra/terraform/tf.sh network plan
#   ./infra/terraform/tf.sh gke apply -auto-approve
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
set -a; . "$root/.env"; set +a

stack=$1; shift
dir="$root/infra/terraform/envs/dev/$stack"
[ -d "$dir" ] || { echo "no stack called $stack" >&2; exit 1; }

export TF_VAR_project_id="$GCP_PROJECT"
export TF_VAR_region="$GCP_REGION"
export TF_VAR_zone="$GCP_ZONE"
export TF_VAR_state_bucket="${GCP_PROJECT}-tfstate"
export TF_VAR_billing_account="${GCP_BILLING_ACCOUNT:-}"
export TF_VAR_budget_amount="${GCP_BUDGET_AMOUNT:-30}"
export TF_VAR_budget_currency="${GCP_BUDGET_CURRENCY:-USD}"
if [ "$stack" = stores-vm ]; then
  export TF_VAR_admin_cidr="$(curl -s --max-time 5 https://api.ipify.org)/32"
  export TF_VAR_ssh_public_key="$(cat "$HOME/.ssh/fraudstream_gcp.pub")"
fi

if [ "$stack" = bootstrap ]; then
  terraform -chdir="$dir" init -input=false >/dev/null
else
  terraform -chdir="$dir" init -input=false -backend-config="bucket=$TF_VAR_state_bucket" >/dev/null
fi
terraform -chdir="$dir" "$@"
