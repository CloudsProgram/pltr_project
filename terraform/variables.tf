locals {
  data_lake_bucket = "pltr_data_lake"
}

variable "project" {
    description = "Your GCP pltr project id"
}

variable "region" {
    description = "Region for GCP resources"
    default = "us-west1"
    type = string
}

variable "storage_class" {
    description = "Storage class type for my bucket"
    default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset, will write data from GCS into BQ"
  type = string
  default = "pltr_stock_info"
}