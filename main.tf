provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-function-bucket"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket_object" "function_code" {
  name   = "function.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = "${path.module}/function.zip"
}

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
    available_memory = "256M"
    timeout_seconds  = 60
    ingress_settings = "ALLOW_ALL"
  }
}

output "function_url" {
  value = google_cloudfunctions2_function.hello_function.service_config[0].uri
}
