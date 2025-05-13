module "flex_template_deployment" {
  source                = "./flex-template-deployment" # Path to the directory where FlexTemplateDeploymentModule is located
  depends_on            = [module.artifactregistry_flex_template_repo, module.dataflow_flex_template_bucket]
  bucket_name           = local.bucket_name
  isolation_bucket_name = local.isolation_bucket_name
  project_id            = local.project_id
  isolation_project_id  = local.isolation_project_id
  tnt_code              = local.tnt_code
  subtnt_env_code       = var.subtnt_env_code
  gcr_repo_name         = local.gcr_repo_name
  sdk_language          = local.sdk_language
  s5_vault_role_id      = var.s5_vault_role_id
  env_name_map          = local.env_name_map
  s5_vault_secret_id    = var.s5_vault_secret_id
  kv_reader_role_id     = var.kv_reader_role_id
  kv_reader_secret_id   = var.kv_reader_secret_id
  PIPELINE_RUN_ID       = var.PIPELINE_RUN_ID
  azure_devops_pat      = local.azure_devops_pat
}