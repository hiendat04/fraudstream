output "name" {
  value = module.platform.name
}

output "zone" {
  value = module.platform.zone
}

output "external_ip" {
  value = module.platform.external_ip
}

output "internal_ip" {
  value = module.platform.internal_ip
}

output "internal_dns" {
  value = module.platform.internal_dns
}

output "models_bucket" {
  value = google_storage_bucket.models.name
}
