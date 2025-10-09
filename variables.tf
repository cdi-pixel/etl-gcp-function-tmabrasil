variable "project_id" {
  type        = string
  description = "ID do projeto GCP (ex.: tmabrasil)"
}

variable "region" {
  type        = string
  description = "Região (ex.: us-central1)"
  default     = "us-central1"
}

variable "function_name" {
  type        = string
  description = "Nome da Cloud Function v2"
  default     = "xlsx-folder-trigger"
}

variable "runtime" {
  type        = string
  description = "Runtime da função (ex.: python312, nodejs22)"
  default     = "python312"
}

variable "entry_point" {
  type        = string
  description = "Nome da função/handler no código"
  default     = "entryPoint"
}

variable "code_bucket_name" {
  type        = string
  description = "Bucket que armazena o ZIP da função (código)"
  default     = "cf-src-bucket"
}

variable "xlsx_bucket_name" {
  type        = string
  description = "Bucket que recebe os .xlsx e dispara a função"
  default     = "cf-xlsx-bucket"
}

variable "object_match" {
  type        = string
  description = "Padrão do objeto para o gatilho (ex.: '*.xlsx' ou 'minha-pasta/*.xlsx')"
  default     = "*.xlsx"
}

variable "memory" {
  type        = string
  description = "Memória de execução (ex.: 256M, 512M, 1Gi)"
  default     = "1Gi"
}

variable "timeout_seconds" {
  type        = number
  description = "Timeout em segundos"
  default     = 1200
}

variable "manage_runtime_sa_bindings" {
  type        = bool
  default     = true
  description = "Se true, o Terraform garante os papéis na runtime SA (eventarc/run/artifact)."
}

variable "manage_gcs_pubsub_binding" {
  type        = bool
  default     = true
  description = "Se true, o Terraform cria o binding roles/pubsub.publisher p/ SA do GCS no projeto."
}

variable "bq_dataset" {
  type        = string
  description = "Dataset do BigQuery onde será criada a tabela base_geral"
}
