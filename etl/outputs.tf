# outputs.tf

output "etl_project_id" {
  description = "ETL project id"
  value       = data.terraform_remote_state.init_ws.outputs.etl_project_id
}

output "dataflow_flex_template_bucket" {
  description = "Data Flow Flex Template Bucket"
  value       = module.dataflow_flex_template_bucket.storage_bucket_name
}

output "dataflow_staging_bucket" {
  description = "DatafLow Staging Bucket"
  value       = module.dataflow_staging_bucket.storage_bucket_name
}

output "dataflow_temp_bucket" {
  description = "Dataflow Temp Bucket"
  value       = module.dataflow_temp_bucket.storage_bucket_name
}

output "service_account_dataflow_oracle_harwick_batch" {
  description = "Dataflow Service Account"
  value       = module.service_account_dataflow_oracle_harwick_batch.name
}

output "service_account_dataflow_data_validation_batch" {
  description = "Dataflow Service Account"
  value       = module.service_account_dataflow_data_validation_batch.name
}

output "service_account_dataflow_fhir_batch" {
  description = "Dataflow Service Account"
  value       = module.service_account_dataflow_fhir_batch.name

}

output "debug_pipeline_trigger" {
  value = {
    pipeline_base_url   = local.azure_pipeline_base_url
    pat_token           = local.azure_devops_pat
    pipeline_status_url = local.azure_pipeline_status_url
  }
  sensitive = false # Allow output visibility for debugging
}
