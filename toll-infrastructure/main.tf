terraform {
  backend "gcs" {
    bucket = "playground-272019-tf-state"
    prefix = "toll-application"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.28.0"
    }
  }
}

provider "google" {
  project = "playground-272019"
  region  = "europe-west1"
}

provider "google-beta" {
  project = "playground-272019"
  region  = "europe-west1"
}

resource "google_artifact_registry_repository" "toll-application-registry-repository" {
  provider = google-beta

  repository_id = "toll-application"
  format        = "DOCKER"

  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"
    condition {
      tag_state  = "UNTAGGED"
      older_than = "604800s" // 7 days
    }
  }
}
resource "google_storage_bucket" "toll-application-bucket" {
  name          = "playground-272019-toll-application"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30 // days
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "toll-application-dataset" {
  dataset_id = "toll_application"
}

resource "google_pubsub_topic" "toll-booth-entry-topic" {
  name = "toll-booth-entry"
}

resource "google_pubsub_subscription" "toll-booth-entry-subscription" {
  name  = "toll-booth-entry"
  topic = google_pubsub_topic.toll-booth-entry-topic.id
}

resource "google_bigquery_table" "toll-booth-entry-table" {
  table_id                 = "toll-booth-entry"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true
  time_partitioning {
    type  = "DAY"
    field = "entry_time"
  }

  schema = file("${path.module}/schemas/toll-booth-entry.json")
}

resource "google_pubsub_topic" "toll-booth-exit-topic" {
  name = "toll-booth-exit"
}

resource "google_pubsub_subscription" "toll-booth-exit-subscription" {
  name  = "toll-booth-exit"
  topic = google_pubsub_topic.toll-booth-exit-topic.id
}

resource "google_bigquery_table" "toll-booth-exit-table" {
  table_id                 = "toll-booth-exit"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "exit_time"
  }

  schema = file("${path.module}/schemas/toll-booth-exit.json")
}

resource "google_pubsub_topic" "vehicle-registration-topic" {
  name = "vehicle-registration"
}

resource "google_pubsub_subscription" "vehicle-registration-subscription" {
  name  = "vehicle-registration"
  topic = google_pubsub_topic.vehicle-registration-topic.id
}

resource "google_bigquery_table" "vehicle-registration-table" {
  table_id                 = "vehicle-registration"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type = "DAY"
  }

  schema = file("${path.module}/schemas/vehicle-registration.json")
}

resource "google_bigquery_table" "toll-booth-entry-stats-table" {
  table_id                 = "toll-booth-entry-stats"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-entry-stats.json")
}

resource "google_bigquery_table" "toll-booth-entry-stats-hourly-table" {
  table_id                 = "toll-booth-entry-stats-hourly"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-entry-stats.json")
}

resource "google_bigquery_table" "toll-booth-entry-stats-daily-table" {
  table_id                 = "toll-booth-entry-stats-daily"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-entry-stats.json")
}

resource "google_bigquery_table" "total-vehicle-times-table" {
  table_id                 = "total-vehicle-times"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/total-vehicle-times.json")
}

resource "google_bigquery_table" "total-vehicle-times-diagnostic-table" {
  table_id                 = "total-vehicle-times-diagnostic"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_bigquery_table" "total-vehicle-times-one-hour-gap-table" {
  table_id                 = "total-vehicle-times-one-hour-gap"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/total-vehicle-times.json")
}

resource "google_bigquery_table" "total-vehicle-times-diagnostic-one-hour-gap-table" {
  table_id                 = "total-vehicle-times-diagnostic-one-hour-gap"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_pubsub_topic" "vehicles-with-expired-registration-topic" {
  name = "vehicles-with-expired-registration"
}

resource "google_bigquery_table" "vehicles-with-expired-registration-table" {
  table_id                 = "vehicles-with-expired-registration"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/vehicles-with-expired-registration.json")
}

resource "google_bigquery_table" "vehicles-with-expired-registration-diagnostic-table" {
  table_id                 = "vehicles-with-expired-registration-diagnostic"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_bigquery_table" "vehicles-with-expired-registration-daily-table" {
  table_id                 = "vehicles-with-expired-registration-daily"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/vehicles-with-expired-registration.json")
}

resource "google_bigquery_table" "vehicles-with-expired-registration-diagnostic-daily-table" {
  table_id                 = "vehicles-with-expired-registration-diagnostic-daily"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/toll-booth-diagnostic.json")
}

resource "google_bigquery_table" "io-diagnostic-table" {
  table_id                 = "io-diagnostic"
  dataset_id               = google_bigquery_dataset.toll-application-dataset.dataset_id
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  schema = file("${path.module}/schemas/diagnostic.json")
}
