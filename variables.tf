variable "project_id" {
  type        = string
  description = "ID do projeto GCP"
}

variable "region" {
  type        = string
  default     = "us-central1"
}

variable "credentials_file" {
  type        = string
  description = "Caminho para o arquivo de credenciais JSON"
}
