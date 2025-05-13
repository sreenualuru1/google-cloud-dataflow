import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import fileio
from google.cloud import bigquery
from datetime import datetime, timezone
from typing import Iterator, Any
import re
import argparse
import logging


class FinalCountBQ(beam.DoFn):
    """
    A DoFn that creates a final reconciliation record by adding table name & load date
    """
    def process(self, element: dict, table: str, load_date: str) -> Iterator[dict]:
        """
        Adds table name, load date & duplicate count to the counts dict for writing to BQ

        Args:
            element (dict): Record containing source, target & invalid count
            table (str) : Table name for which the counts were calculated
            load_date (str): Date of data file received as argument to dataflow job

        Yields:
            dict: Record containing all the reconciliation details
        """
        final_dict = element
        final_dict['MISMATCH_COUNT'] = final_dict['SOURCE_COUNT'] - final_dict['TARGET_COUNT']
        final_dict['TABLE_NAME'] = table
        final_dict['LOAD_DATE'] = load_date 
        yield final_dict

class BQExecuteQuery(beam.DoFn):
    """
    A DoFn that runs the query & returns the output for recon table
    """
    def process(self, query: str) -> Iterator[dict]:
        """
        Runs the query and returns a dict with a record count in the table

        Args:
            query (str) : Query to run to get count of records in target table

        Yields:
            dict: Record containing target table count
        """
        client = bigquery.Client(project=bq_recon_table_project)
        # Run the query
        rows = client.query(query, location='US').result()
        recon_table = {'TARGET_COUNT': 0}
        for row in rows:
            recon_table['TARGET_COUNT'] = row.get('TARGET_COUNT')

        logging.info(f"{query} : executed successfully.")
        yield recon_table
    
# DoFn to match the file pattern exactly as we have scenarios like DW_JOB_D and DW_JOB_D_TL
class FilesMatchPattern(beam.DoFn):
    """
    A DoFn that filters the files based on regex and return file path attribute from FileObject
    """
    def process(self, file, pattern):
        if re.match(pattern, file.path):
            yield file.path

class InsertToBQ(beam.DoFn):
    """
    A DoFn that inserts the row to desired BQ table
    """
    def process(self, record: dict, table: str) -> Iterator[str]:
        """
        Inserts a record into BigQuery table

        Args:
            record (dict) : Final reconciliation record
            table (str) : Table for which reconciliation record is being inserted
        """
        client = bigquery.Client(project=bq_recon_table_project)
        table_id = f'{bq_recon_table_project}.{bq_recon_table_dataset}.{bq_recon_table}'
    
        errors = client.insert_rows_json(table_id, [record])

        # If insert query fails to insert rows then error should be logged and raised accordingly
        if errors:
            logging.error(f"Encountered errors while inserting recon for {table}: {errors}")
            raise Exception(f'BQ write failed for {table}')
        else:
            logging.info(f"Reconciliation record for {table} inserted")
        yield ''

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
    
class MergeDicts(beam.CombineFn):
    """
    A CombineFn that merges the inputs.
    """
    def create_accumulator(self):
        """Initializes the accumulator."""
        return {}

    def add_input(self, accumulator: dict, input: dict):
        """Adds an input element to the accumulator."""
        accumulator.update(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        """Merges multiple accumulators into one."""
        result = {}
        for acc in accumulators:
            result.update(acc)
        return result

    def extract_output(self, accumulator: dict):
        """Extracts the final output from the accumulator."""
        return accumulator

# used for logging the records
def log_data(record):
    logging.info(record)  

def run(known_args, pipeline_args):
    """
    Runs the Beam pipeline to load data stored on GCS to BigQuery.

    Args:
        known_args : A dictionary of arguments that are known/defined in the script
        pipeline_args : A list of unknown arguments that are not defined in the script
    """

    table_name = known_args.table_name
    data_file_prefix = known_args.data_file_prefix
    file_format = known_args.file_format
    type_of_load = known_args.type_of_load
    file_load_date = known_args.data_load_date

    # Setting pipeline options from arguments.
    pipeline_options = PipelineOptions(pipeline_args + ['--project', known_args.project])
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        # Pattern to match exact file path as we have scenarios like DW_JOB_D and DW_JOB_D_TL
        pattern = f"{data_file_prefix}" + r"_\d+_" + f"{file_load_date}T" 
        
        # Read rows according on file format
        if file_format == 'json':                
            data_file_prefix += '*.json'
        else:
            raise Exception(f'{file_format} file format not supported !!')
        
        data = (
                p   | "ListFiles" >> fileio.MatchFiles(data_file_prefix)
                    | "Reshuffle" >> beam.Reshuffle()
                    | "GetFileNames" >> beam.ParDo(FilesMatchPattern(), pattern)
                    | "ReadFromFiles" >> beam.io.ReadAllFromText()
            )

        if type_of_load == 'incremental':
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            query = f"SELECT count(*) as TARGET_COUNT FROM `{bq_project}.{bq_dataset}.{table_name}` where DATE(RECORD_INSERT_DT_TM) = '{today}' or DATE(RECORD_UPDATE_DT_TM) = '{today}'"
        else:
            query = f'SELECT count(*) as TARGET_COUNT FROM `{bq_project}.{bq_dataset}.{table_name}`'
        
        # Counting source records
        source_count = data | f'CountSourceRows' >> CalculateCounts('SOURCE_COUNT')
        
        # Counting target records stored on BQ
        target_count = (p   | f'CreateQuery' >> beam.Create([query])
                            | f'CountTargetRows' >> beam.ParDo(BQExecuteQuery()))

        # Return final reconciliation record for BQ write
        recon_record = (
            (source_count, target_count)
                | f"MergeSourceTarget" >> beam.Flatten()
                | f"Combine" >> beam.CombineGlobally(MergeDicts())
                | f"ReconcileRecord" >> beam.ParDo(FinalCountBQ(), table_name, data_load_date)
        ) 

        # Logging
        source_count | "LogSourceCount" >> beam.Map(log_data)
        target_count | "LogTargetCount" >> beam.Map(log_data)
        recon_record | "LogFinalRecord" >> beam.Map(log_data)

        # BQ Write
        recon_record | f'WriteCountsToBigQuery' >> beam.ParDo(InsertToBQ(), table_name)
        

if __name__ == '__main__':
    
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_name', required=True, type=str)
    parser.add_argument('--data_file_prefix', required=True, type=str)
    parser.add_argument('--file_format', required=True, type=str)
    parser.add_argument('--type_of_load', required=True, type=str)
    parser.add_argument('--bq_dataset', required=True, type=str)
    parser.add_argument('--bq_project', required=True, type=str)
    parser.add_argument('--data_load_date', required=True, type=str)
    parser.add_argument('--project', required=True, type=str)
    parser.add_argument('--bq_recon_table_project',required=True, type=str)
    parser.add_argument('--bq_recon_table_dataset',required=True, type=str)
    parser.add_argument('--bq_recon_table_name',required=True, type=str)
    
    known_args, pipeline_args = parser.parse_known_args()

    # Globally used variables
    bq_project = known_args.bq_project
    bq_dataset = known_args.bq_dataset

    bq_recon_table_project = known_args.bq_recon_table_project
    bq_recon_table_dataset = known_args.bq_recon_table_dataset
    bq_recon_table = known_args.bq_recon_table_name
    data_load_date = datetime.strptime(known_args.data_load_date, '%Y%m%d').strftime('%Y-%m-%d')

    if known_args.type_of_load not in ['full', 'incremental']:
        raise argparse.ArgumentTypeError(f"type_of_load: must be either full or incremental, but provided '{known_args.type_of_load}'")
    
    run(known_args, pipeline_args)

