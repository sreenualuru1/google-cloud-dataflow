#######################################################
#  DataFlow Flex Template Artifact Registry Creation  #
#######################################################

locals {
  # Define the repositories with appropriate details
  repos = {
    "adl-dataflow-gcr-${var.subtnt_env_code}" = {
      format      = "DOCKER"
      location    = var.location
      labels      = {}
      description = "Artifact registry repo for holding dataflow artifacts."
    }
  }
}

module "artifactregistry_flex_template_repo" {
  source        = "tfe.mayo.edu/mcc/m-artifactregistry/google"
  version       = "2.0.0"
  for_each      = local.repos
  project       = local.project_id
  location      = each.value.location
  repository_id = each.key
  description   = each.value.description
  format        = each.value.format
  labels        = each.value.labels
}
