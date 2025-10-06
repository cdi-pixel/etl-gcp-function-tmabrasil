output "function_url" {
  value       = google_cloudfunctions2_function.hello_function.service_config[0].uri
  description = "Endpoint HTTP público da Cloud Function"
}
