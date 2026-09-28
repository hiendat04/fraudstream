output "network_name" {
  value = google_compute_network.this.name
}

output "network_id" {
  value = google_compute_network.this.id
}

output "subnet_id" {
  value = google_compute_subnetwork.this.id
}

output "subnet_cidr" {
  value = google_compute_subnetwork.this.ip_cidr_range
}

output "pods_range_name" {
  value = google_compute_subnetwork.this.secondary_ip_range[0].range_name
}

output "pods_cidr" {
  value = google_compute_subnetwork.this.secondary_ip_range[0].ip_cidr_range
}

output "services_range_name" {
  value = google_compute_subnetwork.this.secondary_ip_range[1].range_name
}
