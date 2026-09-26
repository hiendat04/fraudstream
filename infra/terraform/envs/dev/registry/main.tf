module "registry" {
  source = "../../../modules/registry"
  name   = "fraudstream"
  region = var.region
}
