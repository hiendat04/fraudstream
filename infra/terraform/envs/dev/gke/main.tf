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
  machine_type        = "e2-highmem-2"
  node_count          = 2
  # Agent Substrate installs gVisor on the node, which COS's read-only root does not allow.
  image_type   = "UBUNTU_CONTAINERD"
  disk_size_gb = 50
  # The trial caps SSD at 300 GB a region and the platform VM takes 200 of it; HDD boot
  # disks leave the rest for the volumes pods ask for, on GKE's default SSD class.
  disk_type = "pd-standard"
}
