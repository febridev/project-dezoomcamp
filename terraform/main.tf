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

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# resource "google_storage_bucket" "data-temp-bucket" {
#   name          = "data-temp-bucket-${var.project}" # Concatenating DL bucket & Project name for unique naming
#   location      = var.region

#   # Optional, but recommended settings:
#   storage_class = var.storage_class
#   uniform_bucket_level_access = true

#   versioning {
#     enabled     = true
#   }

#   lifecycle_rule {
#     action {
#       type = "Delete"
#     }
#     condition {
#       age = 30  // days
#     }
#   }

#   force_destroy = true
# }

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}



# resource "google_dataproc_cluster" "simplecluster" {
#   name   = var.DATAPROC_CLUSTERNAME
#   region = var.region

#   cluster_config {
#     # staging_bucket = "data-temp-bucket-${var.project}"

#     master_config {
#       num_instances = 1
#       machine_type  = "n1-standard-2"
#       disk_config {
#         boot_disk_type    = "pd-standard"
#         boot_disk_size_gb = 500
#       }
#     }

#     software_config {
#       image_version = "2.0-debian10"
#       override_properties = {
#         "dataproc:dataproc.allow.zero.workers" = "true"
#       }
#     }

#     gce_cluster_config {
#       # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
#       service_account = var.SERVICE_ACCOUNT
#       networkUri= "default"
#       subnetworkUri= ""
#       internalIpOnly= "false"
#       zoneUri= "asia-southeast2-c"
#       metadata= {}
#       tags= []
#       shieldedInstanceConfig= {
#         "enableSecureBoot"= "false"
#         "enableVtpm"= "false"
#         "enableIntegrityMonitoring"= "false"
#       }
#     }
#   }
# }


