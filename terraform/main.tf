terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.20.0"
    }
  }
}

provider "google" {
  project     = var.Project
  credentials = file(var.gcs_bq_credentials)
}

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.region

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
