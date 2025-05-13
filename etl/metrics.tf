#######################################################
#          Create Metrics in the ETL Project          #
#######################################################

############ FHIR Doc Ref Project ############
resource "google_logging_metric" "fhir_int_docref_inline_df_uri_pull_error_metric" {

  project     = data.terraform_remote_state.init_ws.outputs.etl_project_id
  name        = "fhr_int_docref_indoc_df_uri_pull_err_met"
  description = "Metric for errors when pulling uri's from gcs in Dataflow"
  filter      = <<-EOT
       "DOC-REF-ID: 19456."
  EOT
  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "Encountered an error when pulling uri's from gcs in Dataflow"
  }
}


resource "google_logging_metric" "fhir_int_docref_inline_df_parquet_create_error_metric" {

  project     = data.terraform_remote_state.init_ws.outputs.etl_project_id
  name        = "fhr_int_docref_indoc_df_prqt_crt_err_met"
  description = "Metric for errors when creating parquet files in Dataflow"
  filter      = <<-EOT
       "DOC-REF-ID: 95596."
  EOT
  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "Encountered an error when creating parquet files in Dataflow"
  }
}


resource "google_logging_metric" "fhir_int_docref_inline_df_parquet_upload_error_metric" {

  project     = data.terraform_remote_state.init_ws.outputs.etl_project_id
  name        = "fhr_int_docref_indoc_df_prqt_upl_err_met"
  description = "Metric for errors when uploading parquet files to GCS from Dataflow"
  filter      = <<-EOT
       "DOC-REF-ID: 25750."
  EOT
  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "Encountered an error when uploading parquet files to GCS from Dataflow"
  }
}
######### ^^^ FHIR Doc Ref Project ^^^ #########


############ Harwick BICC Inc Project ############


#          DATA FLOW CODED ERRORS          #
resource "google_logging_metric" "harwick_bicc_dataflow_error_metric" {

  project     = data.terraform_remote_state.init_ws.outputs.etl_project_id
  name        = "harwick_bicc_dataflow_error_metric"
  description = "Metric for errors related to all Dataflow Jobs in the Harwick BICC Incremental DataFlow Project"
  filter      = <<-EOT
    textPayload=~"HBICC|HBII|GENDECOM|HBIM|HBIR|HBIA"
    severity=ERROR
  EOT
  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "Encountered a Harwick BICC Incremental Dataflow Pipeline Error"
  }
}

#          DATA FLOW CODED WARNINGS          #
resource "google_logging_metric" "harwick_bicc_dataflow_warning_metric" {

  project     = data.terraform_remote_state.init_ws.outputs.etl_project_id
  name        = "harwick_bicc_dataflow_warning_metric"
  description = "Metric for warning related to all Dataflow Jobs in the Harwick BICC Incremental DataFlow Project"
  filter      = <<-EOT
    jsonPayload.message=~"PIPELINE WARNING: Ordinal position of"
    severity=WARNING
    resource.type="dataflow_step"
  EOT
  label_extractors = {
    "job_name"   = "EXTRACT(resource.labels.job_name)"
    "project_id" = "EXTRACT(resource.labels.project_id)"
    "msg"        = "EXTRACT(jsonPayload.message)"
  }
  metric_descriptor {
    metric_kind  = "DELTA"
    value_type   = "INT64"
    unit         = "1"
    display_name = "Encountered a Harwick BICC Incremental Dataflow Pipeline Warning"
    labels {
      key        = "job_name"
      value_type = "STRING"
    }
    labels {
      key        = "project_id"
      value_type = "STRING"
    }
    labels {
      key        = "msg"
      value_type = "STRING"
    }
  }
}
######### ^^^ Harwick BICC Inc Project ^^^ #########