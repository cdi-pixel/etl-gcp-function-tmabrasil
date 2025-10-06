terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = var.gcp_credentials_json
}

# Bucket que armazenará o código da função
resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-function-bucket"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Upload do ZIP para o bucket
resource "google_storage_bucket_object" "function_code" {
  name   = "function.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = "${path.module}/function.zip"
}

# Função Cloud Function (Gen2, HTTP)
resource "google_cloudfunctions2_function" "hello_function" {
  name        = "hello-world-function"
  location    = var.region
  description = "Função Python Hello World via Terraform"

  build_config {
    runtime     = "python312"
    entry_point = "hello_world"

    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.function_code.name
      }
    }
  }

  service_config {
    available_memory   = "256M"
    max_instance_count = 3
    timeout_seconds    = 60
    ingress_settings   = "ALLOW_ALL"
  }

  labels = {
    environment = "dev"
    managed_by  = "terraform"
  }
}

output "function_url" {
  value = google_cloudfunctions2_function.hello_function.service_config[0].uri
}
