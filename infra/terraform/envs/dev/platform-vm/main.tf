data "terraform_remote_state" "network" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "dev/network"
  }
}

module "platform" {
  source         = "../../../modules/vm"
  project_id     = var.project_id
  name           = "fraudstream-platform"
  zone           = var.zone
  machine_type   = "e2-highmem-4"
  disk_size_gb   = 200
  network_name   = data.terraform_remote_state.network.outputs.network_name
  subnet_id      = data.terraform_remote_state.network.outputs.subnet_id
  admin_cidr     = var.admin_cidr
  ssh_user       = "fraudstream"
  ssh_public_key = var.ssh_public_key
  client_cidrs = [
    data.terraform_remote_state.network.outputs.subnet_cidr,
    data.terraform_remote_state.network.outputs.pods_cidr,
  ]
  # The stores the pods use, then the UIs behind the gateway.
  service_ports = [
    "5432", "15432", "6379", "9000", "5000", "8200", "29092", "7687", "18082", "18000",
    "18081", "18086", "9002", "18080", "19001", "3000",
  ]
}

# Jenkins on the VM pushes images and deploys to the cluster.
resource "google_project_iam_member" "platform" {
  for_each = toset(["roles/artifactregistry.writer", "roles/container.developer"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${module.platform.service_account_email}"
}

resource "google_storage_bucket" "models" {
  name                        = "${var.project_id}-models"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}

resource "google_storage_bucket_iam_member" "platform" {
  bucket = google_storage_bucket.models.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${module.platform.service_account_email}"
}
