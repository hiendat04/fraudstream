output "cluster_name" {
  value = module.gke.cluster_name
}

output "zone" {
  value = module.gke.zone
}

output "gateway_ip" {
  value = google_compute_address.gateway.address
}
