
######################################################################
#          Oracle Harwick DataFlow Service Account Creation          #
######################################################################
module "service_account_dataflow_oracle_harwick_batch" {
  source       = "tfe.mayo.edu/mcc/m-serviceaccount/google"
  version      = "2.0.1"
  project_id   = local.project_id
  level        = var.service_account_level
  tnt_code     = local.tnt_code
  tnt_tier     = local.tnt_tier
  env          = var.subtnt_env_code
  purpose      = "df-ora-hrwk"
  display_name = "Service Account for Oracle Harwick DataFlow Batch"

}

######################################################################
#          Data Validation DataFlow Service Account Creation         #
######################################################################
module "service_account_dataflow_data_validation_batch" {
  source       = "tfe.mayo.edu/mcc/m-serviceaccount/google"
  version      = "2.0.1"
  project_id   = local.project_id
  level        = var.service_account_level
  tnt_code     = local.tnt_code
  tnt_tier     = local.tnt_tier
  env          = var.subtnt_env_code
  purpose      = "df-data-val"
  display_name = "Service Account for Data Validation DataFlow Batch"

}


######################################################################
#          FHIR Resources DataFlow Service Account Creation          #
######################################################################
module "service_account_dataflow_fhir_batch" {
  source       = "tfe.mayo.edu/mcc/m-serviceaccount/google"
  version      = "2.0.1"
  project_id   = local.project_id
  level        = var.service_account_level
  tnt_code     = local.tnt_code
  tnt_tier     = local.tnt_tier
  env          = var.subtnt_env_code
  purpose      = "df-fhir-bq"
  display_name = "Service Account for FHIR DataFlow Batch"

}


