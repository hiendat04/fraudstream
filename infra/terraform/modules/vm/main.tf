resource "google_service_account" "vm" {
  account_id   = var.name
  display_name = "VM ${var.name}"
}

resource "google_project_iam_member" "vm" {
  for_each = toset(["roles/logging.logWriter", "roles/monitoring.metricWriter"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_compute_instance" "this" {
  name                      = var.name
  zone                      = var.zone
  machine_type              = var.machine_type
  tags                      = [var.name]
  labels                    = { role = "stores" }
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 30
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = var.subnet_id
    access_config {}
  }

  metadata = {
    ssh-keys       = "${var.ssh_user}:${var.ssh_public_key}"
    enable-oslogin = "FALSE"
  }

  service_account {
    email  = google_service_account.vm.email
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_firewall" "ssh" {
  name          = "${var.name}-ssh-admin"
  network       = var.network_name
  direction     = "INGRESS"
  source_ranges = [var.admin_cidr]
  target_tags   = [var.name]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "clients" {
  name          = "${var.name}-from-cluster"
  network       = var.network_name
  direction     = "INGRESS"
  source_ranges = var.client_cidrs
  target_tags   = [var.name]

  allow {
    protocol = "tcp"
    ports    = var.service_ports
  }
}
