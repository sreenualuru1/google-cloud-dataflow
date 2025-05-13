import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import logging
from typing import Any
from google.cloud import storage
from datetime import datetime
import json
import re
import argparse


def generate_dtype_mapping(schema):
    """
    Generates a datatype(dtype) mapping for the schema.
    Args:
        schema (list): The schema to generate datatype mapping for.
    Returns:
        dict: A mapping of column names to their respective datatype.
    """
    dtype_mapping = {}
    for col in schema:
        if col['type'] == 'INTEGER':
            dtype_mapping[col['name']] = 'int64'
        elif col['type'] in ['NUMERIC', 'BIGNUMERIC']:
            dtype_mapping[col['name']] = 'float64'
        else:
            dtype_mapping[col['name']] = 'str'
    
    return dtype_mapping
    
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

class ParseDateColumns(beam.DoFn):
    """
    A DoFn to parse the datetime columns if any and newline characters from exception columns
    """
    def process(self, record, columns, columns_with_chars):
        try:
            record = record._asdict()
            for column in columns:
                if record[column] == None:
                    continue
                col_value = datetime.strptime(record[column], "%Y-%m-%dT%H:%M:%S.%f%z")
                formatted_year = f"{col_value.year}".zfill(4)
                record[column] = col_value.strftime(f"{formatted_year}-%m-%dT%H:%M:%S.%f")

            # Removes newline char from the value for given columns
            for column in columns_with_chars:
                if record[column]:
                    record[column] = record[column].replace('\n', '')

            yield beam.Row(**record)
        except Exception as e:
            logging.error(f"Error for row {record}")
            raise Exception(f"Error while parsing datetime column : {str(e)}")

class UpdateFileName(beam.DoFn):
    """
    A DoFn to rename the output data files to include .csv extension
    """
    def process(self, file, bucket_name, path):
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.get_blob(path + file)
            if blob:
                bucket.rename_blob(blob, path + file + ".csv")
                logging.info(f"gs://{bucket_name}{path + file} renamed to gs://{bucket_name}{path + file}.csv")
        except Exception as e:
            logging.error(f"Error for file {path + file}.csv")
            raise Exception(f"Error while renaming the file : {str(e)}")

# used for logging the records
def log_data(record):
    logging.info(record)


def run(known_args, pipeline_args):
    """
    Run the Apache Beam pipeline to read data, process it, and write it to Parquet.

    Args:
        known_args : A dictionary of arguments that are known/defined in the script
        pipeline_args : A list of unknown arguments that are not defined in the script
    """
    data_file_prefix = known_args.data_file_prefix
    output_path = known_args.output_path
    schema_path = known_args.schema_path

    # Details required to rename the output files
    pattern = r'gs://([^/]+)/(.*/)([^/]+)'
    output_bucket = re.match(pattern, output_path).group(1)
    output_path_prefix = re.match(pattern, output_path).group(2)

    schema =  json.loads(get_content(schema_path))['schema']
    
    # Generate dtype mapping for reading data from csv
    dtype_mapping = generate_dtype_mapping(schema)

    # Information required for reading & parsing data
    columns = [field['name'] for field in schema]
    datetime_columns = [col['name'] for col in schema if col['type'] in ['DATETIME', 'TIMESTAMP']]

    # Intersection performed to check if any exception column present in table schema
    columns_with_newline_chars = list(set(columns) & set(exception_columns)) 

    # Setting pipeline options from arguments.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True   
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        data = (
            pipeline | 'ReadCsv' >> beam.io.ReadFromCsv(data_file_prefix, 
                                                usecols=columns,
                                                keep_default_na=False, 
                                                na_values=[''],
                                                dtype=dtype_mapping,
                                            ) 
        )

        # Total Record Count Logging
        total_count = (
            data | f"TotalRecordCount" >> CalculateCounts('total_record_count')
                 | f"LogTotalCount" >> beam.Map(log_data)
        )

        # This will remove identical duplicates
        distinct_data = (
            data | 'RemoveDuplicates' >> beam.Distinct()
        )

        # Distinct Record Count Logging
        distinct_count = (
            distinct_data | f"DistinctRecordCount" >> CalculateCounts('distinct_record_count')
                 | f"LogDistinctCount" >> beam.Map(log_data)
        )

        # Datetime values are being passed with +00:00 from source which does not support on BQ
        # Hence the ParDo will parse the datetime columns
        if datetime_columns or columns_with_newline_chars:
            parsed = (
                distinct_data | 'ParseDates' >> beam.ParDo(ParseDateColumns(), datetime_columns, columns_with_newline_chars)
            )
        else:
            parsed = distinct_data
        
        # Final write to CSV. beam.Select makes sure the Pcollection is schema-aware which is a mandatory thing for WriteToCsv transform
        final_data = (
            parsed  | "SelectFields" >> beam.Select(*columns)
                    | "WriteToCsv" >> beam.io.WriteToCsv(
                                            output_path,
                                            header = columns,
                                            lineterminator='\n'
                                        )
        )

        # file_name_suffix is not supported in WriteToCsv transform hence the file names to be changed using ParDo to include .csv
        final_data['files_written'] | "UpdateFileName" >> beam.ParDo(UpdateFileName(), output_bucket, output_path_prefix)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_file_prefix', required=True, type=str)
    parser.add_argument('--output_path', required=True, type=str)
    parser.add_argument('--schema_path',required=True, type=str)
    
    known_args, pipeline_args = parser.parse_known_args()

    exception_columns = ['DESCRIPTION']
    run(known_args, pipeline_args)