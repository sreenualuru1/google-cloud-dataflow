################################################################
#                  Default Variables                           #
################################################################

variable "s5_vault_role_id" {
  description = "The Vault ROLE_ID for approle login using initiative S5 service account. Set as secret variable in release pipeline."
  type        = string
}

variable "s5_vault_secret_id" {
  description = "The Vault SECRET_ID for approle login using initiative S5 service account. Set as secret variable in release pipeline."
  type        = string
}

variable "vault_address" {
  description = "The base URL for Vault endpoints."
  type        = string
  default     = "https://vault.mayo.edu:8200"
}

variable "location" {
  description = "Location of the Storage Bucket"
  type        = string
  default     = "us-central1"
}

variable "subtnt_env_code" {
  description = "The environment code (e.g., d, p) for the flex template deployment."
  type        = string
}

variable "project_id" {
  description = "The GCP Project ID where the Flex Template resources will be created."
  type        = string
  default     = "etl_project_id"
}

variable "kv_reader_role_id" {
  description = "The Vault ROLE_ID for approle login using the vault reader account. Set as secret variable in release pipeline."
  type        = string
}

variable "kv_reader_secret_id" {
  description = "The Vault SECRET_ID for approle login using the vault reader account. Set as secret variable in release pipeline."
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

################################################################
#                   Service Account Variables                  #
################################################################
variable "service_account_level" {
  description = "The privilege level of the service account (e.g., S6, S5)."
  type        = string
  default     = "s6"
}

################################################################
#                  GCS Bucket Variables                        #
################################################################
variable "force_destroy" {
  description = "The data owner for the bucket"
  type        = bool
  default     = true
}

################################################################
#                  Flex Template Variables                        #
################################################################
variable "sdk_language" {
  description = "The SDK language to be used for Dataflow Flex Templates"
  type        = string
  default     = "PYTHON"
}

variable "PIPELINE_RUN_ID" {
  description = "ID of the Pipeline"
  type        = string
  default     = ""
}
