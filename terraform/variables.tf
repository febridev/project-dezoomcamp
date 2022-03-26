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
  default = "trips_data_all"
}

variable "VM_AIRFLOW" {
    description = "VM Instance For AirFlow"
    type = string
    default = "airflow-dezoomcamp"
}

variable "VM_MACHINE_TYPE" {
    description = "VM Instance For AirFlow Machine Type"
    type = string
    default = "n1-standard-2"
}


variable "VM_MACHINE_ZONE" {
    description = "VM Instance For AirFlow Machine Type"
    type = string
    default = "asia-southeast2-a"
}