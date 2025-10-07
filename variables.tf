variable "project_id" { type = string }
variable "region"     { type = string }
variable "function_name" { type = string }
variable "entry_point"   { type = string }
variable "runtime_service_account" { type = string }
# optional:
# variable "impersonate_sa" { type = string, default = null }
