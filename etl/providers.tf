# Configures a provider that will use the S5 service account provisioned in the initiative level workspace
provider "vault" {
  address = data.terraform_remote_state.init_ws.outputs.vault_address
  auth_login {
    path = data.terraform_remote_state.init_ws.outputs.vault_auth_path
    parameters = {
      # stored in ADO Pipeline Variable
      role_id   = var.s5_vault_role_id
      secret_id = var.s5_vault_secret_id
    }
  }
}

# Configures a provider that will use the vault-reader account to pull in KV data

provider "vault" {
  alias   = "vault-kv-reader"
  address = data.terraform_remote_state.init_ws.outputs.vault_address
  auth_login {
    path = data.terraform_remote_state.init_ws.outputs.vault_auth_path
    parameters = {
      # stored in ADO Pipeline Variable
      role_id   = var.kv_reader_role_id
      secret_id = var.kv_reader_secret_id
    }
  }
}


#Extracting secrets from vault 
data "vault_generic_secret" "secrets" {
  provider = vault.vault-kv-reader
  path     = local.azure_devops_pat_vault
}


# Obtains a token from Vault for the S5 account
data "vault_generic_secret" "s5_vault_token" {
  path = "gcp/token/${data.terraform_remote_state.init_ws.outputs.s5_vault_roleset}"
}

# Attaches the token from the Vault managed S5 to the google provider.
# If additional providers are used (such as Google-Private Preview, beta, etc they would need similar blocks.
provider "google" {
  access_token = data.vault_generic_secret.s5_vault_token.data["token"]
}

provider "google-beta" {
  access_token = data.vault_generic_secret.s5_vault_token.data["token"]
}

#Extracting secrets from vault
data "vault_generic_secret" "oracle_data_validation_secrets" {
  provider = vault.vault-kv-reader
  path     = local.oracle_data_validation_vault
}
