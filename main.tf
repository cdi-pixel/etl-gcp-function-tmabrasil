terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # ✅ Backend remoto no bucket tmastates (precisa existir)
  backend "gcs" {
    bucket = "tmastates"
    prefix = "state/cf2-xlsx"
  }
}

# ---------------- Provider (WIF/ADC - sem credentials file) ----------------
provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------- Habilitar APIs necessárias ----------------
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
  project             = var.project_id
  service             = each.key
  disable_on_destroy  = false
}

# ---------------- Runtime Service Account (já EXISTE) ----------------
data "google_service_account" "runtime" {
  project    = var.project_id
  account_id = "cf-runtime"
}

# Papéis mínimos para a runtime SA (opcionalmente gerenciados pelo TF)
resource "google_project_iam_member" "runtime_eventarc_receiver" {
  count   = var.manage_runtime_sa_bindings ? 1 : 0
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${data.google_service_account.runtime.email}"
}

resource "google_project_iam_member" "runtime_run_invoker" {
  count   = var.manage_runtime_sa_bindings ? 1 : 0
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${data.google_service_account.runtime.email}"
}

resource "google_project_iam_member" "runtime_artifact_reader" {
  count   = var.manage_runtime_sa_bindings ? 1 : 0
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${data.google_service_account.runtime.email}"
}

# ---------------- Buckets ----------------
# 1) Bucket do CÓDIGO (guarda o ZIP)
resource "google_storage_bucket" "code_bucket" {
  name                        = var.code_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle { prevent_destroy = true }

  depends_on = [google_project_service.services]
}

# 2) Bucket dos XLSX (dispara a função)
resource "google_storage_bucket" "xlsx_bucket" {
  name                        = var.xlsx_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true

  lifecycle { prevent_destroy = true }

  depends_on = [google_project_service.services]
}

# Objeto ZIP do código (gerado pelo seu CI em build/function.zip)
resource "google_storage_bucket_object" "function_code" {
  name   = "source-${substr(filemd5("build/function.zip"), 0, 8)}.zip"
  bucket = google_storage_bucket.code_bucket.name
  source = "build/function.zip"

  depends_on = [google_storage_bucket.code_bucket]
}

# ---------------- Eventarc prerequisites ----------------
# SA interna do Storage (publica eventos em Pub/Sub)
data "google_storage_project_service_account" "gcs_sa" {}

# Binding necessário p/ Eventarc receber eventos do GCS
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  count   = var.manage_gcs_pubsub_binding ? 1 : 0
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

# ---------------- Cloud Function v2 (Eventarc - GCS finalized) ----------------
resource "google_cloudfunctions2_function" "fn" {
  name        = var.function_name
  location    = var.region
  description = "Dispara quando .xlsx chega no bucket ${google_storage_bucket.xlsx_bucket.name}"

  build_config {
    runtime     = var.runtime            # ex: "python312"
    entry_point = var.entry_point        # ex: "entryPoint"
    source {
      storage_source {
        bucket = google_storage_bucket.code_bucket.name
        object = google_storage_bucket_object.function_code.name
      }
    }
  }

  service_config {
    service_account_email          = data.google_service_account.runtime.email
    available_memory               = var.memory          # ex: "256M"
    timeout_seconds                = var.timeout_seconds # ex: 120
    ingress_settings               = "ALLOW_INTERNAL_AND_GCLB"
    all_traffic_on_latest_revision = true
  }

  # 🔔 Gatilho via Eventarc (GCS finalized)
  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = data.google_service_account.runtime.email

    # 1) Filtro exato do bucket
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.xlsx_bucket.name
    }

    # 2) Path pattern (use SUBJECT) para limitar pasta/extensão
    # Formato: /projects/_/buckets/<bucket>/objects/<padrão>
    event_filters {
      attribute = "subject"
      operator  = "match-path-pattern"
      value     = "/projects/_/buckets/${google_storage_bucket.xlsx_bucket.name}/objects/${var.object_match}"
    }
  }

  # Garante ordem: APIs + buckets + (bindings opcionais) antes da função/trigger
  depends_on = [
    google_project_service.services,
    google_storage_bucket.code_bucket,
    google_storage_bucket.xlsx_bucket
    google_project_iam_member.gcs_pubsub_publisher
  ]
}
