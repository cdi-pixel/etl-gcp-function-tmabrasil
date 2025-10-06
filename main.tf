terraform {
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
  credentials = file(var.credentials_file)  # usa o sa.json criado no workflow
}

# --- Liga as APIs necessárias ---
locals {
  required_services = [
    "cloudfunctions.googleapis.com",    # Cloud Functions (Gen2)
    "run.googleapis.com",               # Cloud Run
    "artifactregistry.googleapis.com",  # Artifact Registry
    "cloudbuild.googleapis.com",        # Cloud Build
    "storage.googleapis.com",           # Cloud Storage
  ]
}

resource "google_project_service" "required" {
  for_each           = toset(local.required_services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Bucket p/ código
resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-function-bucket"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  # depends_on precisa ser ESTÁTICO (endereços explícitos):
  depends_on = [
    google_project_service.required["cloudfunctions.googleapis.com"],
    google_project_service.required["run.googleapis.com"],
    google_project_service.required["artifactregistry.googleapis.com"],
    google_project_service.required["cloudbuild.googleapis.com"],
    google_project_service.required["storage.googleapis.com"],
  ]
}

# Usa o ZIP gerado no workflow (build/function.zip)
resource "google_storage_bucket_object" "function_code" {
  name   = "source-${substr(filemd5("build/function.zip"), 0, 8)}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = "build/function.zip"

  depends_on = [
    google_storage_bucket.function_bucket,
    google_project_service.required["cloudfunctions.googleapis.com"],
    google_project_service.required["run.googleapis.com"],
    google_project_service.required["artifactregistry.googleapis.com"],
    google_project_service.required["cloudbuild.googleapis.com"],
    google_project_service.required["storage.googleapis.com"],
  ]
}

# Cloud Function Gen2 (HTTP, Python 3.12)
resource "google_cloudfunctions2_function" "fn" {
  name        = var.function_name
  location    = var.region
  description = "Deploy via Terraform de zip construído no GitHub Actions"

  build_config {
    runtime     = "python312"
    entry_point = var.entry_point

    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.function_code.name
      }
    }
  }

  service_config {
    available_memory   = "256M"
    timeout_seconds    = 60
    max_instance_count = 3
    ingress_settings   = "ALLOW_ALL"
  }

  depends_on = [
    google_project_service.required["cloudfunctions.googleapis.com"],
    google_project_service.required["run.googleapis.com"],
    google_project_service.required["artifactregistry.googleapis.com"],
    google_project_service.required["cloudbuild.googleapis.com"],
    google_project_service.required["storage.googleapis.com"],
  ]
}
