variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "function_name" {
  type    = string
  default = "hello-world-function"
}

variable "entry_point" {
  type    = string
  default = "hello_world"
}

# use se estiver autenticando com key JSON gravada como sa.json no runner
variable "credentials_file" {
  type        = string
  default     = "sa.json"
  description = "Arquivo JSON da service account no runner"
}
