variable "project_id" { type = string }
variable "region"     { type = string }
variable "function_name" { type = string }
variable "entry_point"   { type = string }
variable "runtime_service_account" { type = string }
# optional:
# variable "impersonate_sa" { type = string, default = null }

variable "trigger_bucket_name" {
  type        = string
  description = "Nome do bucket que dispara a função (já existente)."
}

# Padrão: qualquer .xlsx em qualquer pasta
variable "object_match" {
  type        = string
  default     = "*.xlsx"
  description = "Padrão dos objetos para path pattern (ex.: 'minha-pasta/*.xlsx')."
}
