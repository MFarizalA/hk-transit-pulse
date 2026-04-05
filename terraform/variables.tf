variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "gcs_location" {
  description = "GCS bucket region"
  type        = string
  default     = "ASIA-EAST2"  # Hong Kong
}

variable "bq_location" {
  description = "BigQuery dataset location (US for free tier)"
  type        = string
  default     = "US"
}
