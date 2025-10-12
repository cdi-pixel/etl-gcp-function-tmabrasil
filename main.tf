terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# DICA: não defina credentials aqui. Deixe o ADC do GitHub Actions assumir.
provider "google" {
  project = var.project_id
  region  = var.region
  # opcional: se você quiser forçar uma SA diferente daquela autenticada no WIF:
  # impersonate_service_account = var.impersonate_sa # ex: "cf-runtime@tmabrasil.iam.gserviceaccount.com"
}

# Bucket p/ código
resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-function-bucket"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Artefato (força atualização quando o zip muda)
resource "google_storage_bucket_object" "function_code" {
  name   = "source-${substr(filemd5("build/function.zip"), 0, 8)}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = "build/function.zip"
}

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
    available_memory        = "512M"
    timeout_seconds         = 540
    max_instance_count      = 3
    ingress_settings        = "ALLOW_ALL"
    # recomendo fixar a SA de runtime:
    service_account_email   = var.runtime_service_account # ex: "cf-runtime@tmabrasil.iam.gserviceaccount.com"
  }
}

