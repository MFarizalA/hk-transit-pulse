terraform {
  required_providers {
    google = {
      source  = "registry.opentofu.org/hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.6.0"
}

provider "google" {
  project = var.project_id
  region  = var.gcs_location
}

resource "google_storage_bucket" "raw" {
  name          = "hk-transit-pulse-raw"
  location      = var.gcs_location
  force_destroy = false

  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = "marts"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "streaming" {
  dataset_id                 = "streaming"
  location                   = var.bq_location
  delete_contents_on_destroy = true
}

resource "google_service_account" "bruin" {
  account_id   = "bruin-pipeline"
  display_name = "Bruin Pipeline Service Account"
}

resource "google_project_iam_member" "bruin_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_bq_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_project_iam_member" "bruin_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.bruin.email}"
}

resource "google_service_account_iam_member" "bruin_act_as" {
  service_account_id = google_service_account.bruin.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.bruin.email}"
}

# ── Streamlit Dashboard Service Account ───────────────────────────────────────
resource "google_service_account" "streamlit" {
  account_id   = "streamlit-dashboard"
  display_name = "Streamlit Dashboard Service Account"
}

resource "google_project_iam_member" "streamlit_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.streamlit.email}"
}

resource "google_project_iam_member" "streamlit_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.streamlit.email}"
}
