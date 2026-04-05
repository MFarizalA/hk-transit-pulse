output "gcs_bucket_name" {
  description = "GCS raw bucket name"
  value       = google_storage_bucket.raw.name
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_marts" {
  description = "BigQuery marts dataset ID"
  value       = google_bigquery_dataset.marts.dataset_id
}

output "service_account_email" {
  description = "Bruin service account email"
  value       = google_service_account.bruin.email
}

