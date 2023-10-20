terraform {
  backend "gcs" {
    bucket = "playground-272019-tf-state"
    prefix = "toll-application"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.2.0"
    }
  }
}

provider "google" {
  project = "playground-272019"
  region  = "eu-west1"
  zone    = "eu-west1-a"
}

resource "google_storage_bucket" "toll-application-bucket" {
  name     = "playground-272019-toll-application"
  location = "EU"
}

resource "google_pubsub_topic" "toll-booth-entry-topic" {
  name = "toll-booth-entry"
}

resource "google_pubsub_subscription" "toll-booth-entry-subscription" {
  name  = "toll-booth-entry"
  topic = google_pubsub_topic.toll-booth-entry-topic.id
}

resource "google_pubsub_topic" "toll-booth-exit-topic" {
  name = "toll-booth-exit"
}

resource "google_pubsub_subscription" "toll-booth-exit-subscription" {
  name  = "toll-booth-exit"
  topic = google_pubsub_topic.toll-booth-exit-topic.id
}

resource "google_pubsub_topic" "vehicle-registration-topic" {
  name = "vehicle-registration"
}

resource "google_pubsub_subscription" "vehicle-registration-subscription" {
  name  = "vehicle-registration"
  topic = google_pubsub_topic.vehicle-registration-topic.id
}

resource "google_bigquery_dataset" "toll-application-dataset" {
  dataset_id = "toll_application"
}

resource "google_bigquery_table" "vehicle-registration-table" {
  table_id   = "vehicle-registration"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/vehicle-registration.json")
}

resource "google_bigquery_table" "toll-booth-entry-stats-table" {
  table_id   = "toll-booth-entry-stats"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/toll-booth-entry-stats.json")
}

resource "google_bigquery_table" "total-vehicle-times-table" {
  table_id   = "total-vehicle-times"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/total-vehicle-times.json")
}

resource "google_bigquery_table" "total-vehicle-times-diagnostic-table" {
  table_id   = "total-vehicle-times-diagnostic"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_pubsub_topic" "vehicles-with-expired-registration-topic" {
  name = "vehicles-with-expired-registration"
}

resource "google_bigquery_table" "vehicles-with-expired-registration-diagnostic-table" {
  table_id   = "vehicles-with-expired-registration-diagnostic"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_bigquery_table" "io-diagnostic-table" {
  table_id   = "io-diagnostic"
  dataset_id = google_bigquery_dataset.toll-application-dataset.dataset_id

  schema = file("${path.module}/schemas/io-diagnostic.json")
}
