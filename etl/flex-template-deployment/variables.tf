# In the flex_template_deployment module's variables.tf
variable "project_id" {
  description = "Project ID for GCP"
  type        = string
}
variable "isolation_project_id" {
  description = "Project ID for Isolation Project"
  type        = string
}
variable "subtnt_env_code" {
  description = "Tenant Sub Code for GCP"
  type        = string
}

variable "env_name_map" {
  description = "root level environment map"
  default = "dev"
}

variable "tnt_code" {
  description = "Tenant Code for GCP"
  type        = string
}
variable "bucket_name" {
  description = "Bucket name for GCP"
  type        = string
}
variable "isolation_bucket_name" {
  description = "Bucket name for isolation project"
  type        = string
}
variable "gcr_repo_name" {
  description = "Bucket name for GCP"
  type        = string
}
variable "sdk_language" {
  description = "The SDK language to be used for Dataflow Flex Templates"
  type        = string
}

variable "PIPELINE_RUN_ID" {
  description = "ID of the Pipeline"
  type        = string
  default     = ""
}
variable "s5_vault_role_id" {
  description = "The Vault ROLE_ID for approle login using initiative S5 service account. Set as secret variable in release pipeline."
  type        = string
}

variable "azure_devops_pat" {
  description = "azure devops pat info"
  type        = string
}

variable "s5_vault_secret_id" {
  description = "The Vault SECRET_ID for approle login using initiative S5 service account. Set as secret variable in release pipeline."
  type        = string
}

variable "kv_reader_role_id" {
  description = "The Vault ROLE_ID for approle login using the vault reader account. Set as secret variable in release pipeline."
  type        = string
}

variable "kv_reader_secret_id" {
  description = "The Vault SECRET_ID for approle login using the vault reader account. Set as secret variable in release pipeline."
  type        = string
}

variable "vault_address" {
  description = "The base URL for Vault endpoints."
  type        = string
  default     = "https://vault.mayo.edu:8200"
}