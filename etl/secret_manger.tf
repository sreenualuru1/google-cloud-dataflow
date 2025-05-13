# Set secrets needed for dataflow deployments

resource "google_secret_manager_secret" "oracle_data_validation_username" {
  project   = lookup(data.terraform_remote_state.init_ws.outputs, var.project_id, null)
  provider  = google-beta
  secret_id = "oracle_data_validation_username"
  labels = {
    label = "oracle_data_validation_username"
  }
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
    }
  }
}

resource "google_secret_manager_secret" "oracle_data_validation_password" {
  project   = lookup(data.terraform_remote_state.init_ws.outputs, var.project_id, null)
  provider  = google-beta
  secret_id = "oracle_data_validation_password"
  labels = {
    label = "oracle_data_validation_password"
  }
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
    }
  }
}

resource "google_secret_manager_secret_version" "oracle_data_validation_username_value" {
  depends_on  = [google_secret_manager_secret.oracle_data_validation_username, google_secret_manager_secret.oracle_data_validation_username]
  secret      = google_secret_manager_secret.oracle_data_validation_username.id
  secret_data = data.vault_generic_secret.oracle_data_validation_secrets.data["username"]
}

resource "google_secret_manager_secret_version" "oracle_data_validation_password_value" {
  depends_on  = [google_secret_manager_secret.oracle_data_validation_password, google_secret_manager_secret.oracle_data_validation_password]
  secret      = google_secret_manager_secret.oracle_data_validation_password.id
  secret_data = data.vault_generic_secret.oracle_data_validation_secrets.data["password"]
}