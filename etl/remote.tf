# remote_state.tf

data "terraform_remote_state" "init_ws" {
  backend = "remote"

  config = {
    organization = "mcc"
    hostname     = "tfe.mayo.edu"
    workspaces = {
      name = "tfe-advance-data-lake-init-${lookup(local.env_name_map, var.subtnt_env_code)}"
    }
  }
}

data "terraform_remote_state" "aide_init_ws" {
  backend = "remote"

  config = {
    organization = "mcc"
    hostname     = "tfe.mayo.edu"
    workspaces = {
      name = "init-adl-aide-${lookup(local.env_name_map_nonprod, var.subtnt_env_code)}"
    }
  }
}

data "terraform_remote_state" "composer_inf_ws" {
  backend = "remote"

  config = {
    organization = "mcc"
    hostname     = "tfe.mayo.edu"
    workspaces = {
      name = "infra-composer-${lookup(local.env_name_map, var.subtnt_env_code)}"
    }
  }
}

data "terraform_remote_state" "iso_ws" {
  backend = "remote"

  config = {
    organization = "mcc"
    hostname     = "tfe.mayo.edu"
    workspaces = {
      name = "app-isolation-${lookup(local.env_name_map, var.subtnt_env_code)}"
    }
  }
}
