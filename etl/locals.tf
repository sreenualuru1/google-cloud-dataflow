# Local values  used across terraform configuration for better modularity and reusability
# Define common variables and complex expressions that are used in multiple resources within this project

locals {
  # Fetching project ID from the remote state for consistent project usage
  project_id           = lookup(data.terraform_remote_state.init_ws.outputs, var.project_id, null)
  isolation_project_id = data.terraform_remote_state.iso_ws.outputs.isolation_project_id

  # Define GCR Repo name dynamically  fetched from artifact registry module
  gcr_repo_name = "us-central1-docker.pkg.dev/${var.project_id}/adl-dataflow-gcr-${var.subtnt_env_code}"

  # Define GCS Bucket name dynamically  fetched from GCS module
  bucket_name               = module.dataflow_flex_template_bucket.storage_bucket_name
  isolation_bucket_name     = data.terraform_remote_state.iso_ws.outputs.dataflow_flex_template_bucket
  azure_devops_pat_vault    = "kv/flex_template_build_pat"
  azure_devops_pat          = data.vault_generic_secret.secrets.data["azure_devops_pat"]
  azure_pipeline_base_url   = "https://dev.azure.com/mclm/MCC%20Advanced%20Data%20Lake/_apis/pipelines/25927/runs?api-version=6.0"
  azure_pipeline_status_url = "https://dev.azure.com/mclm/MCC%20Advanced%20Data%20Lake/_apis/pipelines/25927/runs/RUN_ID?api-version=6.0-preview"
  sdk_language              = var.sdk_language
  tnt_code                  = data.terraform_remote_state.init_ws.outputs.tnt_code
  tnt_tier                  = data.terraform_remote_state.init_ws.outputs.tnt_tier

  # Vault for oracle secrets
  oracle_data_validation_vault = "kv/${lookup(local.env_name_map, var.subtnt_env_code)}/oracle-data-validation-credentials"

  # BICC Helper files
  bicc_helper_files_folder = "./harwick/harwick-helper-file-inc/${lookup(local.env_name_map, var.subtnt_env_code)}"


  env_name_map = {
    "d" = "dev"
    "t" = "test"
    "s" = "stage"
    "p" = "prod"
  }

  branch_map = {
    "d" = "develop"
    "t" = "develop"
    "s" = "master"
    "p" = "master"
  }

  env_name_map_nonprod = {
    "d" = "dev"
    "t" = "test"
    "s" = "nonprod"
    "p" = "prod"
  }

  variable_group_map = {
    "d" = "adl-artreg-vars-dev"
    "t" = "adl-artreg-vars-test"
    "s" = "adl-artreg-vars-stage"
    "p" = "adl-artreg-vars-prod"
  }

  selected_branch         = local.branch_map[var.subtnt_env_code]
  selected_variable_group = local.variable_group_map[var.subtnt_env_code]

  env_ci_map = {
    "d" = {
      "ci" : "ci06307450",
      "em_project" : "ml-mps-cpl-asnevent-d-cebf"
    }
    "t" = {
      "ci" : "ci06629256",
      "em_project" : "ml-mps-cpl-asnevent-t-51a3"
    }
    "s" = {
      "ci" : "ci07028651",
      "em_project" : "ml-mps-cpl-asnevent-t-51a3"
    }
    "p" = {
      "ci" : "ci07120574",
      "em_project" : "ml-mps-cpl-asnevent-p-eccc"
    }
  }
}