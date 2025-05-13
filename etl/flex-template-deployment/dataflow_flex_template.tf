# Define a resource for building a Flex Template

resource "null_resource" "build_flex_template" {

  depends_on = [null_resource.trigger_azure_pipeline]

  # Loop over local project details to execute the build for each project
  for_each = local.project_details
  triggers = {
    directory_hash = local.directory_hashes[each.key]
  }

  provisioner "local-exec" {
    command = <<EOT
     echo "Authenticating with Vault..."
     VAULT_RESPONSE=$(curl --silent --request POST \
       --data '{"role_id": "${var.s5_vault_role_id}", "secret_id": "${var.s5_vault_secret_id}"}' \
       "${var.vault_address}/v1/adl_aide/auth/approle/login")
     echo "Vault Response Raw: $VAULT_RESPONSE"
     VAULT_TOKEN=$(echo $VAULT_RESPONSE | jq -r '.auth.client_token')
     if [ -z "$VAULT_TOKEN" ]; then
       echo "Vault authentication failed: $VAULT_RESPONSE"
       exit 1
     fi

     echo "Retrieving GCP Token..."

     GCP_RESPONSE=$(curl --silent --header "X-Vault-Token: $VAULT_TOKEN" \
       "${var.vault_address}/v1/adl_aide/gcp/token/s5-mgmt-${var.subtnt_env_code}")
     echo "GCP Response Raw: $GCP_RESPONSE"

     GCP_TOKEN=$(echo $GCP_RESPONSE | jq -r '.data.token')
     if [ -z "$GCP_TOKEN" ]; then
       echo "Failed to retrieve GCP token: $GCP_RESPONSE"
       exit 1
     fi

# Set up the environment for Google Cloud SDK (gcloud) using the retrieved token

    export CLOUDSDK_AUTH_ACCESS_TOKEN=$GCP_TOKEN
     echo "Exported GCP Token: $CLOUDSDK_AUTH_ACCESS_TOKEN"

# Build the Flex Template using Google Cloud SDK and the provided parameters

     echo "Building Flex Template..."
     gcloud dataflow flex-template build gs://${lookup(each.value, "bucket_name", var.bucket_name)}/flex-templates/${each.key}.json \
       --image ${lookup(each.value, "gcr_repo_name", local.gcr_repo_name)}/${each.key}:latest \
       --sdk-language ${local.sdk_language} \
       --metadata-file "${each.value.folder}/metadata.json" || exit 1
     echo "Flex Template build completed for ${each.key}" 
   EOT
  }
}