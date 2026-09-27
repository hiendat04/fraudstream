output "name" {
  value = google_compute_instance.this.name
}

output "zone" {
  value = google_compute_instance.this.zone
}

output "external_ip" {
  value = google_compute_instance.this.network_interface[0].access_config[0].nat_ip
}

output "internal_ip" {
  value = google_compute_instance.this.network_interface[0].network_ip
}

output "internal_dns" {
  value = "${google_compute_instance.this.name}.${google_compute_instance.this.zone}.c.${var.project_id}.internal"
}

output "service_account_email" {
  value = google_service_account.vm.email
}
