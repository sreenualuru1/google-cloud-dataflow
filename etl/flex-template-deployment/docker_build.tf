# Define a resource to trigger the Azure DevOps pipeline

resource "null_resource" "trigger_azure_pipeline" {
  # Loop over local project details to trigger the pipeline for each project
  for_each = local.project_details

  # Trigger condition: this resource will run whenever there's change in directory
  triggers = {
    directory_hash = local.directory_hashes[each.key]
  }

  # Provisioner to run a local shell script for triggering the Azure pipeline
  provisioner "local-exec" {
    command = "${path.module}/trigger_and_monitor_pipeline.sh ${each.key} ${each.value.folder}"

    # Pass environment variables needed for the shell script to function
    environment = {
      BRANCH           = local.selected_branch
      GCR_REPO         = lookup(each.value, "gcr_repo_name", local.gcr_repo_name)
      AZURE_DEVOPS_PAT = var.azure_devops_pat
      AZURE_BASE_URL   = local.azure_pipeline_base_url
      AZURE_STATUS_URL = local.azure_pipeline_status_url
      VARIABLE_GROUP   = local.selected_variable_group
    }
  }
}