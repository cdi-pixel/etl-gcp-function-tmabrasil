variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "region" {
  description = "Região da função"
  type        = string
  default     = "us-central1"
}

variable "gcp_credentials_json" {
  description = "Conteúdo JSON das credenciais do Service Account"
  type        = string
  sensitive   = true
}
