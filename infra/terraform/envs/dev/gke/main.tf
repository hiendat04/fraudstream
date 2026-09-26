data "terraform_remote_state" "network" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "dev/network"
  }
}

module "gke" {
  source              = "../../../modules/gke"
  project_id          = var.project_id
  name                = "fraudstream"
  zone                = var.zone
  network_id          = data.terraform_remote_state.network.outputs.network_id
  subnet_id           = data.terraform_remote_state.network.outputs.subnet_id
  pods_range_name     = data.terraform_remote_state.network.outputs.pods_range_name
  services_range_name = data.terraform_remote_state.network.outputs.services_range_name
}

resource "google_compute_address" "gateway" {
  name   = "fraudstream-gateway"
  region = var.region
}
