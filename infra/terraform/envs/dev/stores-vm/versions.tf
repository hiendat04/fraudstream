terraform {
  required_version = ">= 1.16"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.4"
    }
  }
  backend "gcs" {
    prefix = "dev/stores-vm"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  default_labels = {
    project    = "fraudstream"
    env        = "dev"
    managed-by = "terraform"
  }
}
