variable "project_id" {
  type        = string
  description = "ID do projeto GCP"
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
  description = "Nome da função/handler no código (ex.: entryPoint)"
  default     = "entryPoint"
}

variable "code_bucket_name" {
  type        = string
  description = "Nome do bucket que armazena o ZIP da função"
  default     = "cf-src-bucket"
}

variable "xlsx_bucket_name" {
  type        = string
  description = "Nome do bucket que recebe os .xlsx e dispara a função"
  default     = "cf-xlsx-bucket"
}

variable "object_match" {
  type        = string
  description = "Padrão de objetos para o gatilho (ex.: '*.xlsx' ou 'minha-pasta/*.xlsx')"
  default     = "*.xlsx"
}

variable "memory" {
  type        = string
  description = "Memória de execução (ex.: 256M, 512M, 1Gi)"
  default     = "256M"
}

variable "timeout_seconds" {
  type        = number
  description = "Timeout em segundos"
  default     = 120
}
