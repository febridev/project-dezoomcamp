locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-southeast2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "bq_project_dezoomcamp"
}

variable "DATAPROC_CLUSTERNAME" {
  description = "DATAPROC_CLUSTERNAME"
  type = string
  default = "project-dezoomcamp"
}

variable "SERVICE_ACCOUNT" {
  description = "SERVICE ACCOUNT ID FROM IAM"
  type = string
  default = "dezoomcamp@applied-mystery-341809.iam.gserviceaccount.com"
}