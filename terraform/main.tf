terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
        source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

# resource "google_service_account" "default" {
#     account_id = "service_account_id"
#     display_name = "Service Account"
  
# }
resource "google_compute_address" "static" {
  name = "address-dezoomcamp"
}

resource "google_compute_instance" "dezoomcamp" {
  name = var.VM_AIRFLOW
  machine_type = var.VM_MACHINE_TYPE
  zone = var.VM_MACHINE_ZONE

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-minimal-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.static.address
    }
  }


  # service_account {
  #   # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
  #   email  = google_service_account.default.email
  #   scopes = ["cloud-platform"]
  # }
}