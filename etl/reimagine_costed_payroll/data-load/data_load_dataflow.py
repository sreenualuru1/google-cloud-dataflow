import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import fileio
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
from datetime import datetime, timezone
from typing import Iterator, Any
from jsonschema import validate, ValidationError
import re
import argparse
import logging


def get_column_names(schema: dict) -> list:
    """
    Extracts column names from BQ schema

    Args:
    - schema: bigquery schema 

    Returns:
    - List of column names 
    """
    field_list = schema['fields']
    fields = [field['name'] for field in field_list]
    return fields

# Function to create json compatible schema from BQ schema
def bq_to_json_schema(bq_schema: dict) -> dict:
    def convert_field(field: dict) -> dict:
        valid_type = {}
        if field['type'] in ["STRING", "TIMESTAMP", "DATE", "TIME", "DATETIME","BYTES"]:
            valid_type = {'type': ["string"] }
        elif field['type'] in ["FLOAT", "NUMERIC", "DECIMAL", "BIGNUMERIC", "INTEGER", "INT64"]:
            valid_type = {'type': ["number"]}
        elif field['type'] == 'RECORD':
            valid_type = {
                "type": ["object"],
                "properties": {
                    sub_field["name"]: convert_field(sub_field) for sub_field in field["fields"]
                },
                "additionalProperties": False
            }
        else:
            raise Exception(f"{field['type']} is not currently supported in the template.")
        
        if field['mode'] == 'NULLABLE':
            valid_type['type'].append('null')
        
        return valid_type

    return {
        "type": "object",
        "properties": {
            field["name"]: convert_field(field) for field in bq_schema['fields']
        },
        "additionalProperties": False
    }

def get_content(file_path: str) -> str:
    """
    Extracts content from GCS object

    Args:
    - file_path: path to the GCS object

    Returns:
    - Content of the file passed to this function
    """
    pattern = '^gs://([^/]+)/(.*)$'
    path = re.match(pattern, file_path)
    
    if path:
        try:
            logging.info(f'File path pattern matched with bucket {path.group(1)} and path {path.group(2)}')
            client = storage.Client()
            buck = client.bucket(path.group(1))
            blob = buck.get_blob(path.group(2))
            content = blob.download_as_string().decode('utf-8')
            return content
        except Exception as e:
            logging.error(f'Error  while reading file path: {str(e)}')
            raise Exception(f"Incorrect file path : {file_path}")
    else:
        raise Exception(f'Incorrect file path : {file_path}')

# used for logging the records
def log_data(record):
    logging.info(record)

# DoFn to match the file pattern exactly as we have scenarios like DW_JOB_D and DW_JOB_D_TL
class FilesMatchPattern(beam.DoFn):
    """
    A DoFn that filters the files based on regex and return file path attribute from FileObject
    """
    def process(self, file, pattern):
        if re.match(pattern, file.path):
            yield file.path

# Few of the tables have W$ in column name which is not supported in BQ hence renaming is needed
class RenameColumns(beam.DoFn):
    """
    A DoFn to rename the columns if they exist
    """
    def process(self, record):
        element = dict(record)
        if 'W$_INSERT_DT' in element:
            element['SRC_INSERT_DT'] = element.pop('W$_INSERT_DT')
        if 'W$_UPDATE_DT' in element:
            element['SRC_UPDATE_DT'] = element.pop('W$_UPDATE_DT')
        
        yield element

# DoFn for validating each record against predefined schema
# Easiest way to validate a json record rather than creating iteration for checking fields & data type
class ValidateJson(beam.DoFn):
    """
    A DoFn that validates each record against predefined schema.
    """

    def process(self, record: dict, schema: dict) -> Iterator[tuple]:
        """
        Validates an individual element against the schema.

        Args:
            record (dict): Record from GCS file
            schema (dict) : Schema to validate with the record

        Yields:
            tuple: Tuple having partition id & record.
        """
        try:
            validate(record, schema)
            yield (0, record)
        except ValidationError as e:
            record['error_message'] = e.message
            yield (1, record)


class CalculateCounts(beam.PTransform):
    """
    A PTransform that counts number of records in a PCollection.
    """
    def __init__(self, record: str):
        """Initializes the PTransform with a type of record count."""
        self.count_type = record

    def expand(self, pcollection: beam.PCollection[Any]):
        """Expands the PTransform to apply the transformation for counting records"""

        return (pcollection | f'CountRows' >> beam.combiners.Count.Globally()
                            | f'ToDict' >> beam.Map(lambda x: {self.count_type: x})
        )
    

class PostBQWriteDoFn(beam.DoFn):
    """
    A DoFn that runs the merge query & drop the staging table for incremental load tables
    """

    def process(self, element, table: str, primary_key_columns: list, columns: list):
        audit_columns = ['RECORD_INSERT_DT_TM', 'RECORD_UPDATE_DT_TM', 'RECORD_INSERT_BY', 'RECORD_UPDATE_BY']
        
        # Create a match condition for set of primary keys
        condition = " AND ".join([f"T.`{col}` = S.`{col}`" for col in primary_key_columns])

        # Create SET values for all other columns
        columns_to_update = list(set(columns) - set(primary_key_columns + audit_columns))
        set_columns = ", ".join([f"T.`{col}` = S.`{col}`" for col in columns_to_update])

        # Insert would work for all columns hence using columns directly
        insert_columns = ", ".join([f"`{col}`" for col in columns])
        value_columns = ", ".join([f"S.`{col}`" for col in columns])

        merge_query = f"""
            MERGE `{bq_project}.{bq_dataset}.{table}` T
            USING `{bq_project}.{bq_stg_dataset}.STG_{table}` S
            ON {condition}
            WHEN MATCHED THEN
            UPDATE SET {set_columns}, T.RECORD_UPDATE_DT_TM=CURRENT_TIMESTAMP(), T.RECORD_UPDATE_BY='{service_account}'
            WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({value_columns})
        """

        delete_query = f"drop table `{bq_project}.{bq_stg_dataset}.STG_{table}`"

        client = bigquery.Client(project=bq_project)
        pr_query = merge_query.replace('\n', ' ')
        logging.info(f"MERGE QUERY: {pr_query}")
        try:
            # Run the query
            merge_query_job = client.query_and_wait(merge_query, location='US') 
            logging.info(f"Job ID for merge query: {merge_query_job.job_id}")
            logging.info(f"Number of rows affected: {merge_query_job.num_dml_affected_rows}")

            drop_query_job = client.query_and_wait(delete_query, location='US') 
            logging.info(f"Job ID for drop query: {drop_query_job.job_id}")

            logging.info(f"{table} : Merge query executed successfully and staging table dropped.")
            yield 'success'  
        except NotFound as e:
            logging.error(f"Table not found as there was no data written to STG_{table} table")
        except Exception as e:
            raise Exception(f"Merge/Drop query failed : {str(e)}")
            

def run(known_args, pipeline_args):
    """
    Runs the Beam pipeline to load data stored on GCS to BigQuery.

    Args:
        known_args : A dictionary of arguments that are known/defined in the script
        pipeline_args : A list of unknown arguments that are not defined in the script
    """
    table_name = known_args.table_name
    schema_path = known_args.schema_path
    data_file_prefix = known_args.data_file_prefix
    file_format = known_args.file_format
    type_of_load = known_args.type_of_load
    primary_keys = known_args.primary_keys
    primary_key_list = primary_keys.split(",") if primary_keys != "" else []

    if type_of_load == "incremental" and len(primary_key_list) < 1:
        raise Exception(f"No primary key provided for incremental load !")

    # Setting pipeline options from arguments.
    pipeline_options = PipelineOptions(pipeline_args + ['--project', known_args.project, '--service_account_email', service_account])
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        
        # Pattern to match exact file path as we have scenarios like DW_JOB_D and DW_JOB_D_TL
        pattern = f"{data_file_prefix}" + r"_\d+_" + f"{load_date}T" #+ date

        if file_format == 'json':
            data_file_prefix += '*.json'
        else:
            raise Exception(f'{file_format} file format not supported !!')

        # Parse BQ schema from string to dict
        bq_schema = {"fields": json.loads(get_content(schema_path))['schema']}

        # Get json compatible schema from BQ schema for validation
        json_schema = bq_to_json_schema(bq_schema)

        # Match the files with exact file pattern to process
        data = (
            p   | f"ListFiles" >> fileio.MatchFiles(data_file_prefix)
                | f'Reshuffle' >> beam.Reshuffle()
                | f"FilterFiles" >> beam.ParDo(FilesMatchPattern(), pattern)
        )
        
        stg_table = table_name
        columns = []
        if type_of_load == 'incremental':
            stg_table = f'STG_{table_name}'
            columns = get_column_names(bq_schema)

        # Read records from each file and add filename alongside each record Tuple(filename, record)
        data_pcollection = (
            data | f'ReadFromFiles' >> beam.io.ReadAllFromText(with_filename=True)
        )

        # Log the total count of records 
        total_record_count = (
            data_pcollection | f"TotalRecordCount" >> CalculateCounts('source_record_count')
                             | f"LogTotalCount" >> beam.Map(log_data)
        )
        # Log the count of records for each file 
        per_file_count = (
            data_pcollection | f"CountPerFile" >> beam.combiners.Count.PerKey()
                             | f"LogCountPerFile" >> beam.Map(log_data)
        )

        # beamb.Distinct() performs hashing on backend to remove duplicates Tuple((A=B), (C=D))
        # And hence converted to tuple as hashing is not supported on dicts
        deduplicate = (
            data_pcollection| f'ParseRecords' >> beam.Map(lambda x: tuple(sorted(json.loads(x[1]).items())))
                            | f'RemoveDuplicates' >> beam.Distinct()
                    )
        
        # Log the total distinct records
        disinct_count = (
            deduplicate | f"DistinctRecordCount" >> CalculateCounts('distinct_record_count')
                        | f"LogDistinctCount" >> beam.Map(log_data)
        )

        # Idea is to rename W$_INSERT_DT type columns to SRC_INSERT_DT as $ is not supported in column name on BQ
        dict_rows = deduplicate | f'RenameColumns' >> beam.ParDo(RenameColumns())

        # validate function from jsonschema module will help to validate each record (0, record) & (1, record)
        # If a record is invalid, error_msg is added to that record
        validated = dict_rows | f'ValidateSchema' >> beam.ParDo(ValidateJson(), json_schema)
        
        # Splitting the validated records into 2 partitions(valid,invalid) for processing differently
        valid_records, invalid_records = validated | f"SplitValidInvalid" >> beam.Partition(lambda x, _: x[0], 2)

        # Log the total distinct records
        valid_count = (
            valid_records | f"ValidRecordCount" >> CalculateCounts('valid_record_count')
                          | f"LogValidCount" >> beam.Map(log_data)
        )

        # json.dumps was required as while writing to GCS it should be a valid json record again otherwise None would be returned as is instead of null
        # Write rows which don't follow schema to GCS
        (invalid_records    | f'InvalidToJson' >> beam.Map(lambda x: json.dumps(x[1]))
                            | f"WriteInvalidToGCS" >> beam.io.WriteToText(
                                                    file_path_prefix = error_path + table_name, # Error Path - gs://error_folder/table_name/YYYYMMDD/
                                                    file_name_suffix='.json',
                                                    skip_if_empty=True
                                                ))
        
        # Write rows matching schema to BQ after addig audit columns
        bq_write_pcol = (valid_records  
                            | f'AddAuditColumns' >> beam.Map(
                                                        lambda x: {**x[1], 
                                                                "RECORD_INSERT_DT_TM": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
                                                                "RECORD_UPDATE_DT_TM": None,
                                                                "RECORD_INSERT_BY": service_account,
                                                                "RECORD_UPDATE_BY": None
                                                            }
                                                    ) 
                            | f'WriteValidToBigQuery' >> beam.io.WriteToBigQuery(
                                                            table=stg_table,
                                                            dataset=bq_stg_dataset if type_of_load=='incremental' else bq_dataset,
                                                            additional_bq_parameters = {'timePartitioning': {"field": "RECORD_INSERT_DT_TM", 'type': 'MONTH'}},
                                                            project=bq_project,
                                                            schema=bq_schema,
                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                            custom_gcs_temp_location=f'{bq_temp_location}{stg_table}/'
                                                        ))

        # In case of incremental load, performing merge on staging & main table and dropping staging table
        if type_of_load == 'incremental':
            (
             (bq_write_pcol.destination_load_jobid_pairs, bq_write_pcol.destination_copy_jobid_pairs)
                            | f'CollectLoadCopyJobId' >> beam.Flatten()
                            | f"WaitForBQWrite" >> beam.GroupByKey()
                            | f"RunOnlyOnce" >> beam.combiners.ToList()
                            | f"RunMergeAndDropStg" >> beam.ParDo(PostBQWriteDoFn(), table_name, primary_key_list, columns)
            )

if __name__ == '__main__':
    
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, type=str)
    parser.add_argument('--table_name', required=True, type=str)
    parser.add_argument('--data_load_date', required=True, type=str)
    parser.add_argument('--data_file_prefix', required=True, type=str)
    parser.add_argument('--file_format', required=True, type=str)
    parser.add_argument('--type_of_load', required=True, type=str)
    parser.add_argument('--primary_keys', required=True, type=str)
    parser.add_argument('--schema_path', required=True, type=str)
    parser.add_argument('--bq_project', required=True, type=str)
    parser.add_argument('--bq_dataset', required=True, type=str)
    parser.add_argument('--bq_stg_dataset', required=True, type=str)
    parser.add_argument('--error_path', required=True, type=str)
    parser.add_argument('--service_account_email', required=True, type=str)
    parser.add_argument('--bq_temp_location', required=True, type=str)
   
    known_args, pipeline_args = parser.parse_known_args()

    # Globally used variables
    bq_project = known_args.bq_project
    bq_dataset = known_args.bq_dataset
    bq_stg_dataset = known_args.bq_stg_dataset
    bq_temp_location = known_args.bq_temp_location
    error_path = known_args.error_path
    service_account = known_args.service_account_email
    load_date = known_args.data_load_date

    config_pattern = r"^gs://.+/$"

    if not re.match(config_pattern, known_args.error_path):
        raise argparse.ArgumentTypeError(f"error_path: must follow gs://bucket_name/path_to_error_folder/ ")
    if known_args.type_of_load not in ['full', 'incremental']:
        raise argparse.ArgumentTypeError(f"type_of_load: must be either full or incremental, but provided '{known_args.type_of_load}'")
    
    run(known_args, pipeline_args)