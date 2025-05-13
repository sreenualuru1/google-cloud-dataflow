##########################################
#   Local Variables                      #
##########################################

locals {
  dataflow_service_accounts = [
    "serviceAccount:${module.service_account_dataflow_oracle_harwick_batch.name}",
    "serviceAccount:${module.service_account_dataflow_data_validation_batch.name}",
    "serviceAccount:${module.service_account_dataflow_fhir_batch.name}"
    # Add more service accounts here as needed in the future
  ]

  contractor_user_account = [
    "group:${data.terraform_remote_state.init_ws.outputs.contractor_adl_group_email}"
  ]
  mayo_user_account = [
    "group:${data.terraform_remote_state.init_ws.outputs.mayo_adl_group_email}"
  ]
  user_account = var.subtnt_env_code == "p" ? local.mayo_user_account : concat(local.contractor_user_account, local.mayo_user_account)

  #--------------------------------------
  # Artifact Registry Locals Definition
  #--------------------------------------
  artreg_consumers = concat(
    local.dataflow_service_accounts,
    local.user_account
  )

  # Ensure IAM bindings are created only for existing repositories
  artreg_iam_bindings = {
    for key, repo in local.repos : key => {
      repository_id = try(module.artifactregistry_flex_template_repo[key].id, null)
      consumers     = local.artreg_consumers
    } if try(module.artifactregistry_flex_template_repo[key].id, null) != null
  }
}

##########################################
#   Human Resources IAM Editor           #
##########################################

module "user_iam_permissions" {
  source   = "tfe.mayo.edu/mcc/m-iam-project/google"
  projects = [local.project_id]
  version  = "3.0.0"
  enabled  = var.subtnt_env_code == "d" || var.subtnt_env_code == "t" ? true : false
  mode     = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.iam.serviceaccountuser" = local.user_account,
    "roles/storage.objectAdmin"                                                                            = local.user_account,
    "roles/dataflow.admin"                                                                                 = local.user_account
  }
}

#--------------------------------------------------------------------------------------------
# DEV ONLY, NON-CONTRACTOR ONLY permission to delete artifacts in the dev artifact registry
#--------------------------------------------------------------------------------------------
module "user_iam_permissions_dev_only" {
  source   = "tfe.mayo.edu/mcc/m-iam-project/google"
  projects = [local.project_id]
  version  = "3.0.0"
  enabled  = var.subtnt_env_code == "d" ? true : false
  mode     = "additive"
  bindings = {
    "roles/artifactregistry.repoAdmin" = ["group:${data.terraform_remote_state.init_ws.outputs.mayo_adl_group_email}"],
    "roles/monitoring.editor"          = ["group:${data.terraform_remote_state.init_ws.outputs.mayo_adl_group_email}"]
  }
}

#####################################################
#      DataFlow Service Account IAM Permissions     #
#####################################################

module "dataflow_service_accounts_iam_permissions" {
  source   = "tfe.mayo.edu/mcc/m-iam-project/google"
  projects = [local.project_id]
  version  = "3.0.0"
  enabled  = true
  mode     = "additive"
  bindings = {
    "roles/dataflow.admin"               = local.dataflow_service_accounts,
    "roles/dataflow.worker"              = local.dataflow_service_accounts,
    "roles/dataflow.serviceAgent"        = local.dataflow_service_accounts,
    "roles/storage.objectAdmin"          = local.dataflow_service_accounts,
    "roles/logging.logWriter"            = local.dataflow_service_accounts,
    "roles/monitoring.metricWriter"      = local.dataflow_service_accounts,
    "roles/compute.viewer"               = local.dataflow_service_accounts,
    "roles/compute.networkUser"          = local.dataflow_service_accounts,
    "roles/secretmanager.secretAccessor" = local.dataflow_service_accounts
  }
}


#################################################################
#       Apply IAM on the Artifact Registry repositories         #
#################################################################

module "m-iam-artifactregistry" {
  source  = "tfe.mayo.edu/mcc/m-iam-artifactregistry/google"
  version = "2.0.1"

  # Iterate only over repositories that have been created
  for_each = local.repos

  # Use the repository ID from the created repositories
  repository = module.artifactregistry_flex_template_repo[each.key].id
  depends_on = [
    module.artifactregistry_flex_template_repo
  ]

  mode = "additive"

  # Ensure IAM bindings are created based on the provided bindings
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.artifactregistry.reader" = local.artreg_consumers
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.artifactregistry.writer" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.vault_sa_artreg_publisher_email}"
    ]
  }
}

#################################################################
#       Apply IAM on the Artifact Registry Publisher            #
#################################################################

# Apply project level permissions to the artifact registry publisher so it can build flex templates
module "iam_bindings_artreg_publisher_project_level_build_permissions" {
  source  = "tfe.mayo.edu/mcc/m-iam-project/google"
  version = "3.0.0"

  projects = [local.project_id]
  mode     = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.dataflow.developer" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.vault_sa_artreg_publisher_email}"
    ],
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.compute.viewer" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.vault_sa_artreg_publisher_email}"
    ]
  }
}

# Apply permissions to the flex template bucket so the flex template build account can write there during flex template build
module "iam_bindings_artreg_publisher_storage_build_permissions" {
  source  = "tfe.mayo.edu/mcc/m-iam-storage/google"
  version = "3.0.0"

  depends_on = [module.dataflow_flex_template_bucket]

  storage_buckets = [module.dataflow_flex_template_bucket.storage_bucket_name]
  mode            = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.storage.objectadmin" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.vault_sa_artreg_publisher_email}"
    ],
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.storage.bucketviewer" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.vault_sa_artreg_publisher_email}"
    ]
  }
}

#################################################################
#         Apply IAM on cloud composer service accounts          #
#################################################################

module "flex_template_deploy_project_iam" {
  source  = "tfe.mayo.edu/mcc/m-iam-project/google"
  version = "3.0.0"

  depends_on = [module.dataflow_flex_template_bucket]

  # Disable anything that uses values pulled in from composer remote state in test
  enabled = var.subtnt_env_code == "t" ? false : true

  projects = [local.project_id]

  mode = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.dataflow.developer" = [
      "serviceAccount:${data.terraform_remote_state.composer_inf_ws.outputs.service_account_composer_orchestration}"
    ],
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.compute.viewer" = [
      "serviceAccount:${data.terraform_remote_state.composer_inf_ws.outputs.service_account_composer_orchestration}"
    ],
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.iam.serviceaccountuser" = [
      "serviceAccount:${data.terraform_remote_state.composer_inf_ws.outputs.service_account_composer_orchestration}"
    ]
  }
}

module "google_cloud_storage_sa_iam_editor_composer" {
  source  = "tfe.mayo.edu/mcc/m-iam-storage/google"
  version = "3.0.0"

  # Disable anything that uses values pulled in from composer remote state in test
  enabled = var.subtnt_env_code == "t" ? false : true

  storage_buckets = [module.dataflow_flex_template_bucket.storage_bucket_name, module.dataflow_temp_bucket.storage_bucket_name, module.dataflow_staging_bucket.storage_bucket_name]
  mode            = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.storage.objectadmin" = [
      "serviceAccount:${data.terraform_remote_state.composer_inf_ws.outputs.service_account_composer_orchestration}"
    ]
  }
}

###################################################################
#      Apply IAM on Dataflow accounts for pre-existing resources  #
###################################################################

module "docref_df_bigquery_dataset_iam_bindings" {

  source  = "tfe.mayo.edu/mcc/m-iam-bigquery-dataset/google"
  version = "5.0.0"

  project     = data.terraform_remote_state.aide_init_ws.outputs.fhir_integration_phi_project_id
  dataset_ids = ["phi_primary_use_fhir_clinicnumber_us_${var.subtnt_env_code}"]
  mode        = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.bigquery.dataeditor" = [
      "serviceAccount:${module.service_account_dataflow_fhir_batch.name}"
    ]
  }
}

module "docref_df_project_iam_bindings" {

  source  = "tfe.mayo.edu/mcc/m-iam-project/google"
  version = "3.0.0"

  projects = [data.terraform_remote_state.aide_init_ws.outputs.fhir_integration_phi_project_id]

  mode = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.bigquery.jobuser" = [
      "serviceAccount:${module.service_account_dataflow_fhir_batch.name}"
    ]
  }
}


###################################################################
#      Apply IAM on S5 SA to access Flex Template Buckets  #
###################################################################

module "google_cloud_storage_sa_iam_editor_s5" {
  source  = "tfe.mayo.edu/mcc/m-iam-storage/google"
  version = "3.0.0"

  # Disable anything that uses values pulled in from composer remote state in test
  enabled = var.subtnt_env_code == "d" ? false : true

  storage_buckets = [module.dataflow_flex_template_bucket.storage_bucket_name, module.dataflow_temp_bucket.storage_bucket_name]
  mode            = "additive"
  bindings = {
    "organizations/${data.terraform_remote_state.init_ws.outputs.org_id}/roles/mcc.storage.objectadmin" = [
      "serviceAccount:${data.terraform_remote_state.init_ws.outputs.s5_service_account_email}"
    ]
  }
}