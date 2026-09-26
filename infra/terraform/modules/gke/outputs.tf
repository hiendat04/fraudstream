output "cluster_name" {
  value = google_container_cluster.this.name
}

output "zone" {
  value = google_container_cluster.this.location
}
