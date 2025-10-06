variable "project_id"     { type = string }
variable "region"         { type = string, default = "us-central1" }
variable "function_name"  { type = string, default = "hello-world-function" }
variable "entry_point"    { type = string, default = "hello_world" }

# Apenas se usar SA JSON (como no workflow acima)
variable "credentials_file" {
  type        = string
  default     = "sa.json"
  description = "Arquivo gerado no runner a partir do secret GCP_SA_KEY"
}
