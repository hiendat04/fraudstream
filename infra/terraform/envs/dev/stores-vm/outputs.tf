output "name" {
  value = module.stores.name
}

output "zone" {
  value = module.stores.zone
}

output "external_ip" {
  value = module.stores.external_ip
}

output "internal_ip" {
  value = module.stores.internal_ip
}

output "internal_dns" {
  value = module.stores.internal_dns
}

output "models_bucket" {
  value = google_storage_bucket.models.name
}
