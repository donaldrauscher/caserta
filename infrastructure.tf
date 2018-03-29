variable "project" {}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-f"
}

provider "google" {
  version = "~> 1.8"
  project = "${var.project}"
  region = "${var.region}"
}

resource "google_storage_bucket" "caserta" {
  name = "djr-caserta"
  storage_class = "REGIONAL"
  location = "${var.region}"
  lifecycle {
    prevent_destroy = "True"
  }
}

resource "google_bigquery_dataset" "crypto_currencies" {
  dataset_id = "crypto_currencies"
}
