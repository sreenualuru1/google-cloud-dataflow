###############################################################################
#          Create Alerting Notification Channels in the ETL Project           #
###############################################################################

# Notification topic for the ITSM Event Management integration with Service Now
module "service_now_notification_channel" {

  source  = "tfe.mayo.edu/mcc/m-stackdriver-notification-channel/google"
  version = "2.0.1"

  project_id = local.project_id
  org        = data.terraform_remote_state.init_ws.outputs.org_name
  tnt_code   = local.tnt_code
  purpose    = "service-now"
  type       = "pubsub"
  enabled    = true
  labels = {
    topic = "projects/${lookup(lookup(local.env_ci_map, var.subtnt_env_code), "em_project")}/topics/ml-int-cpl-monitor-amonitor"
  }
  user_labels = {
    classification = data.terraform_remote_state.init_ws.outputs.label_classification_phi
    data_owner     = data.terraform_remote_state.init_ws.outputs.label_data_owner
  }
  description = "Notification channel for ITSM Event Management in the ADL ETL project"
}
