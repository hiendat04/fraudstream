terraform {
  required_version = ">= 1.16"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.4"
    }
  }
}

provider "google" {
  project               = var.project_id
  region                = var.region
  user_project_override = true
  billing_project       = var.project_id
  default_labels = {
    project    = "fraudstream"
    env        = "dev"
    managed-by = "terraform"
  }
}
