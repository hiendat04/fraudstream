module "network" {
  source        = "../../../modules/network"
  name          = "fraudstream"
  region        = var.region
  subnet_cidr   = "10.10.0.0/20"
  pods_cidr     = "10.20.0.0/16"
  services_cidr = "10.30.0.0/20"
}
