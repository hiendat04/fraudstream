output "network_name" {
  value = module.network.network_name
}

output "network_id" {
  value = module.network.network_id
}

output "subnet_id" {
  value = module.network.subnet_id
}

output "subnet_cidr" {
  value = module.network.subnet_cidr
}

output "pods_range_name" {
  value = module.network.pods_range_name
}

output "pods_cidr" {
  value = module.network.pods_cidr
}

output "services_range_name" {
  value = module.network.services_range_name
}
