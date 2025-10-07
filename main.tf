terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "tmastates"               # <- você disse que vai criar esse bucket p/ state
    prefix = "state/cf2-xlsx"          # organiza o tfstate dentro do bucket
  }
}

# ---------------- Provider ----------------
provider "google" {
  project = var.project_id
  region  = var.region
  # Nada de credentials aqui (WIF/ADC do GitHub Actions vai cuidar)
}

# ---------------- Enable APIs (idempotente) ----------------
resource "google_project_service" "services" {
  for_each = toset([
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
  ])
  project = var.project_id
  service = each.key

  disable_on_destroy = false
}

# ---------------- Service Account de runtime ----------------
resource "google_service_account" "runtime" {
  account_id   = "cf-runtime"
  display_name = "Cloud Functions Runtime"
}

# Papéis mínimos para a runtime SA (execução da CF v2 + Eventarc + imagem)
resource "google_project_iam_member" "runtime_eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_project_iam_member" "runtime_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_project_iam_member" "runtime_artifact_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

# ---------------- Buckets ----------------
# 1) Bucket do código da função (onde o ZIP será enviado)
resource "google_storage_bucket" "code_bucket" {
  name                        = var.code_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true

  # Não apague sem querer
  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.services]
}

# 2) Bucket de entrada (.xlsx) que dispara a função
resource "google_storage_bucket" "xlsx_bucket" {
  name                        = var.xlsx_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.services]
}

# Objetinho ZIP do código (Terraform envia ./build/function.zip)
resource "google_storage_bucket_object" "function_code" {
  name   = "source-${substr(filemd5("build/function.zip"), 0, 8)}.zip"
  bucket = google_storage_bucket.code_bucket.name
  source = "build/function.zip"

  depends_on = [google_storage_bucket.code_bucket]
}

# ---------------- Eventarc prerequisites ----------------
# SA interna do Storage (publicador de eventos)
data "google_storage_project_service_account" "gcs_sa" {}

# GCS precisa publicar em Pub/Sub (Eventarc usa isso)
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

# ---------------- Cloud Function v2 (event-driven por GCS) ----------------
resource "google_cloudfunctions2_function" "fn" {
  name        = var.function_name
  location    = var.region
  description = "CF v2: dispara quando .xlsx chega no bucket ${var.xlsx_bucket_name}"

  build_config {
    runtime     = var.runtime
    entry_point = var.entry_point

    source {
      storage_source {
        bucket = google_storage_bucket.code_bucket.name
        object = google_storage_bucket_object.function_code.name
      }
    }
  }

  service_config {
    service_account_email          = google_service_account.runtime.email
    available_memory               = var.memory
    timeout_seconds                = var.timeout_seconds
    ingress_settings               = "ALLOW_INTERNAL_AND_GCLB"
    all_traffic_on_latest_revision = true
  }

  # 🔔 Gatilho via Eventarc (GCS finalized)
  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.runtime.email

    # 1) Filtro exato pelo bucket que recebe os .xlsx
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.xlsx_bucket.name
    }

    # 2) (Opcional) Path pattern para restringir por pasta/arquivo usando SUBJECT
    # Formato: /projects/_/buckets/<bucket>/objects/<pasta ou padrão>
    event_filters {
      attribute = "subject"
      operator  = "match-path-pattern"
      value     = "/projects/_/buckets/${google_storage_bucket.xlsx_bucket.name}/objects/${var.object_match}"
    }
  }

  # Garante ordem: APIs + buckets + publisher antes da função/trigger
  depends_on = [
    google_project_service.services,
    google_storage_bucket.code_bucket,
    google_storage_bucket.xlsx_bucket,
    google_project_iam_member.runtime_eventarc_receiver,
    google_project_iam_member.runtime_run_invoker,
    google_project_iam_member.runtime_artifact_reader,
    google_project_iam_member.gcs_pubsub_publisher
  ]
}
