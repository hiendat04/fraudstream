data "terraform_remote_state" "network" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "dev/network"
  }
}

module "stores" {
  source         = "../../../modules/vm"
  project_id     = var.project_id
  name           = "fraudstream-stores"
  zone           = var.zone
  network_name   = data.terraform_remote_state.network.outputs.network_name
  subnet_id      = data.terraform_remote_state.network.outputs.subnet_id
  admin_cidr     = var.admin_cidr
  ssh_user       = "fraudstream"
  ssh_public_key = var.ssh_public_key
  client_cidrs = [
    data.terraform_remote_state.network.outputs.subnet_cidr,
    data.terraform_remote_state.network.outputs.pods_cidr,
  ]
  service_ports = ["5432", "6379", "5000"]
}
