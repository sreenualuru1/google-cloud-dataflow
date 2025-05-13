locals {
  sdk_language = var.sdk_language
  project_details = {

    # HARWICK FLEX TEMPLATE DETAILS
    "ofu-bucket-archive-inc" = {
      folder = "harwick/harwick-bucket-archive-incremental/${lookup(var.env_name_map,var.subtnt_env_code)}"
    }
    "ofu-bq-merge-inc" = {
      folder = "harwick/harwick-bq-merge-incremental/${lookup(var.env_name_map,var.subtnt_env_code)}"
    }
    "ofu-recon-inc" = {
      folder = "harwick/harwick-data-reconciliation-incremental/${lookup(var.env_name_map,var.subtnt_env_code)}"
    }

    # DOCUMENT REFERENCE FLEX TEMPLATE DETAILS
    "fhir-convert-documents-to-base64" = {
      folder = "fhir-integration/document-reference-convert-documents-to-base64"
    }

    # REIMAGINE COSTED PAYROLL FLEX TEMPLATE DETAILS
    "reimagine-costed-payroll-data-archive" = {
      folder = "reimagine_costed_payroll/data-archive"
    }
    "reimagine-costed-payroll-data-load" = {
      folder = "reimagine_costed_payroll/data-load"
    }
    "reimagine-costed-payroll-data-reconciliation" = {
      folder = "reimagine_costed_payroll/data-reconciliation"
    }


    # REIMAGINE BI PUBLISHER FLEX TEMPLATE DETAILS

    "reimagine-bi-publisher-data-archive" = {
      folder = "reimagine_bi_publisher/data-archive"
    }
    "reimagine-bi-publisher-data-load" = {
      folder = "reimagine_bi_publisher/data-load"
    }
    "reimagine-bi-publisher-data-reconciliation" = {
      folder = "reimagine_bi_publisher/data-reconciliation"
    }

    # GENERIC FLEX TEMPLATE DETAILS
    "custom-worker-image-etl-py311-sdk-2-60" = {
      folder = "generic/dataflow-sdk-py11-worker-image"
    }
    "custom-worker-image-py311-sdk-2-60" = {
      folder        = "generic/dataflow-sdk-py11-worker-image"
      gcr_repo_name = "us-central1-docker.pkg.dev/${var.isolation_project_id}/adl-iso-dataflow-gcr-${var.subtnt_env_code}"
      bucket_name   = var.isolation_bucket_name
    }
    "zipfile-decompress" = {
      folder = "generic/zipfile-decompress"
    }
    "s3-to-gcs-sync" = {
      folder        = "generic/s3-to-gcs-sync"
      gcr_repo_name = "us-central1-docker.pkg.dev/${var.isolation_project_id}/adl-iso-dataflow-gcr-${var.subtnt_env_code}"
      bucket_name   = var.isolation_bucket_name
    }

    # Data Validation
    "custom-worker-image-etl-py39-sdk-2-63" = {
      folder = "generic/dataflow-sdk-py39-worker-image"
    }
    "data-validation" = {
      folder = "generic/data-validation"
    }
  }
  gcr_repo_name = "us-central1-docker.pkg.dev/${var.project_id}/adl-dataflow-gcr-${var.subtnt_env_code}"

  azure_pipeline_base_url   = "https://dev.azure.com/mclm/MCC%20Advanced%20Data%20Lake/_apis/pipelines/25927/runs?api-version=6.0"
  azure_pipeline_status_url = "https://dev.azure.com/mclm/MCC%20Advanced%20Data%20Lake/_apis/pipelines/25927/runs/RUN_ID?api-version=6.0-preview"

  directory_hashes = tomap({
    for project, detail in local.project_details :
    project => join("", [for file in fileset(detail.folder, "**/*") : filesha512("${detail.folder}/${file}")])
  })

  branch_map = {
    "d" = "develop"
    "t" = "develop"
    "s" = "master"
    "p" = "master"
  }
  variable_group_map = {
    "d" = "adl-artreg-vars-dev"
    "t" = "adl-artreg-vars-test"
    "s" = "adl-artreg-vars-stage"
    "p" = "adl-artreg-vars-prod"
  }

  selected_branch         = local.branch_map[var.subtnt_env_code]
  selected_variable_group = local.variable_group_map[var.subtnt_env_code]
}