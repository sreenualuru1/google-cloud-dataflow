#######################################################
#          Create Alerts in the ETL Project           #
#######################################################



##############################################################################
#               Alerting for a failed dataflow job in ETL project            #
##############################################################################

resource "google_monitoring_alert_policy" "dataflow_failed" {

  depends_on = [module.service_now_notification_channel]

  project      = local.project_id
  display_name = "ml-adl-dataflow-failed"
  combiner     = "AND"

  conditions {
    display_name = "Dataflow Failed - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" metric.type=\"dataflow.googleapis.com/job/is_failed\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields = [
          "resource.label.job_name"
        ]
      }
    }
  }
  documentation {
    content = "A dataflow job has failed in the ADL ETL project."
  }

  user_labels = {
    project           = local.project_id
    type              = "operations"
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }


  notification_channels = [
    module.service_now_notification_channel.notification_channel_name
  ]
}

#######################################################################################
#               Alerting for a pulling uri's from gcs error in ETL project            #
#######################################################################################

resource "google_monitoring_alert_policy" "fhir_int_docref_inline_df_uri_pull_error_alert" {

  depends_on = [module.service_now_notification_channel]

  project      = data.terraform_remote_state.init_ws.outputs.etl_project_id
  display_name = "fhr-int-docref-indoc-df-uri-pull-err-alt"
  combiner     = "AND"

  conditions {
    display_name = "Document Reference dataflow uri pull error alert - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/fhr_int_docref_indoc_df_uri_pull_err_met\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields = [
          "resource.label.job_name"
        ]
      }
    }
  }
  documentation {
    content = "A Document Reference dataflow uri pull error in the ADL ETL project."
  }

  user_labels = {
    project           = data.terraform_remote_state.init_ws.outputs.etl_project_id
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    type              = "operations"
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }


  notification_channels = [
  ]
}

#######################################################################################
#               Alerting for a parquet file create error in ETL project               #
#######################################################################################

resource "google_monitoring_alert_policy" "fhir_int_docref_inline_df_parquet_create_error_alert" {

  depends_on = [module.service_now_notification_channel]

  project      = data.terraform_remote_state.init_ws.outputs.etl_project_id
  display_name = "fhr-int-docref-indoc-df-prqt-crt-err-alt"
  combiner     = "AND"

  conditions {
    display_name = "Document Reference dataflow parquet create error alert - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/fhr_int_docref_indoc_df_prqt_crt_err_met\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields = [
          "resource.label.job_name"
        ]
      }
    }
  }
  documentation {
    content = "A Document Reference dataflow parquet file create error in the ADL ETL project."
  }

  user_labels = {
    project           = data.terraform_remote_state.init_ws.outputs.etl_project_id
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    type              = "operations"
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }


  notification_channels = [
  ]
}

#######################################################################################
#               Alerting for a parquet file upload error in ETL project               #
#######################################################################################

resource "google_monitoring_alert_policy" "fhir_int_docref_inline_df_parquet_upload_error_alert" {

  depends_on = [module.service_now_notification_channel]

  project      = data.terraform_remote_state.init_ws.outputs.etl_project_id
  display_name = "fhr-int-docref-indoc-df-prqt-upl-err-alt"
  combiner     = "AND"

  conditions {
    display_name = "Document Reference dataflow bigquery upload error alert - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/fhr_int_docref_indoc_df_prqt_upl_err_met\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields = [
          "resource.label.job_name"
        ]
      }
    }
  }
  documentation {
    content = "A Document Reference dataflow parquet file upload error in the ADL ETL project."
  }

  user_labels = {
    project           = data.terraform_remote_state.init_ws.outputs.etl_project_id
    type              = "operations"
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }


  notification_channels = [
  ]
}
#########################################################################################
#               Alerting for failed Harwick BICC Incremental Dataflow Job               #
#########################################################################################

resource "google_monitoring_alert_policy" "harwick_bicc_dataflow_error_alert" {

  depends_on   = [module.service_now_notification_channel, google_logging_metric.harwick_bicc_dataflow_error_metric]
  count        = var.subtnt_env_code == "d" || var.subtnt_env_code == "s" || var.subtnt_env_code == "p" ? 1 : 0
  project      = data.terraform_remote_state.init_ws.outputs.etl_project_id
  display_name = "harwick_bicc_dataflow_error_alert"
  combiner     = "AND"

  conditions {
    display_name = "Dataflow Harwick BICC Incremental Error Alert - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/harwick_bicc_dataflow_error_metric\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields = [
          "resource.label.job_name"
        ]
      }
    }
  }
  documentation {
    content = "A Dataflow Job in the Harwick BICC Incremental Project has failed."
  }

  user_labels = {
    project           = data.terraform_remote_state.init_ws.outputs.etl_project_id
    type              = "operations"
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }


  notification_channels = [
    module.service_now_notification_channel.notification_channel_name
  ]
}

#########################################################################################
#     Alerting for Ordinal Position warning in Harwick BICC Incremental Dataflow Job    #
#########################################################################################

resource "google_monitoring_alert_policy" "harwick_bicc_dataflow_warning_alert" {

  depends_on   = [module.service_now_notification_channel, google_logging_metric.harwick_bicc_dataflow_warning_metric]
  count        = var.subtnt_env_code == "d" || var.subtnt_env_code == "s" || var.subtnt_env_code == "p" ? 1 : 0
  project      = data.terraform_remote_state.init_ws.outputs.etl_project_id
  display_name = "harwick_bicc_dataflow_warning_alert"
  combiner     = "OR"

  conditions {
    display_name = "Dataflow Harwick BICC Incremental Error Alert - ${var.subtnt_env_code}"
    condition_threshold {
      filter     = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/harwick_bicc_dataflow_warning_metric\""
      comparison = "COMPARISON_GT"
      duration   = "0s"
      trigger {
        count = 1
      }
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_COUNT"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields = [
          "resource.labels.job_name",
          "resource.labels.project_id"
        ]
      }
    }
  }
  documentation {
    content = "A Dataflow Job in the Harwick BICC Incremental Project has failed."
  }

  user_labels = {
    project           = data.terraform_remote_state.init_ws.outputs.etl_project_id
    type              = "operations"
    sn_cid            = lookup(lookup(local.env_ci_map, var.subtnt_env_code), "ci")
    sn_severity       = "3"
    sn_sourceinstance = "mcc-data-lake-services-aide-level-3"
    sn_subcategory    = "connectivity"
  }

  alert_strategy {
    auto_close = "86400s"
  }

  notification_channels = [
    module.service_now_notification_channel.notification_channel_name
  ]
}