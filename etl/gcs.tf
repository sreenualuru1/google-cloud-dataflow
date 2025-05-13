#######################################################
#   DataFlow Flex Template GCS Bucket Creation        #
#######################################################


module "dataflow_flex_template_bucket" {
  source  = "tfe.mayo.edu/mcc/m-storage/google"
  version = "3.0.0"

  org            = data.terraform_remote_state.init_ws.outputs.org_name
  classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
  data_source    = "dataflow-flex-template"
  tnt_code       = local.tnt_code
  env            = var.subtnt_env_code
  project_id     = local.project_id
  location       = var.location
  enabled        = var.subtnt_env_code == "d" || var.subtnt_env_code == "t" || var.subtnt_env_code == "s" || var.subtnt_env_code == "p" ? true : false
  labels = {
    classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
    data_owner     = data.terraform_remote_state.init_ws.outputs.label_data_owner
    storage_type   = "dataflow-flex-template"
  }
  force_destroy = var.force_destroy
  # Conditionally enable versioning only in prod
  versioning = var.subtnt_env_code == "p" ? true : false
  soft_delete_policy = {
    retention_duration_seconds = 0
  }
}


# Upload helper files for bicc
resource "google_storage_bucket_object" "bicc_helper_files" {
  depends_on = [module.dataflow_flex_template_bucket]
  for_each   = { for file in fileset(local.bicc_helper_files_folder, "**/*") : file => "${local.bicc_helper_files_folder}/${file}" }

  bucket = module.dataflow_flex_template_bucket.storage_bucket_name
  name   = "flex-template-helper-files/${each.key}"
  source = each.value
}




#######################################################
#         DataFlow Staging GCS Bucket Creation        #
#######################################################


module "dataflow_staging_bucket" {
  source  = "tfe.mayo.edu/mcc/m-storage/google"
  version = "3.0.0"

  org            = data.terraform_remote_state.init_ws.outputs.org_name
  classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
  data_source    = "dataflow-staging"
  tnt_code       = local.tnt_code
  env            = var.subtnt_env_code
  project_id     = local.project_id
  location       = var.location
  labels = {
    classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
    data_owner     = data.terraform_remote_state.init_ws.outputs.label_data_owner
    storage_type   = "dataflow-staging"
  }
  force_destroy = var.force_destroy
  # Conditionally enable versioning only in prod
  versioning = false
  soft_delete_policy = {
    retention_duration_seconds = 0
  }
}


#######################################################
#         DataFlow Temp GCS Bucket Creation        #
#######################################################


module "dataflow_temp_bucket" {
  source  = "tfe.mayo.edu/mcc/m-storage/google"
  version = "3.0.0"

  org            = data.terraform_remote_state.init_ws.outputs.org_name
  classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
  data_source    = "dataflow-temp"
  tnt_code       = local.tnt_code
  env            = var.subtnt_env_code
  project_id     = local.project_id
  location       = var.location
  labels = {
    classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
    data_owner     = data.terraform_remote_state.init_ws.outputs.label_data_owner
    storage_type   = "dataflow-temp"
  }
  force_destroy = var.force_destroy
  # Conditionally enable versioning only in prod
  versioning = false
  soft_delete_policy = {
    retention_duration_seconds = 0
  }
}