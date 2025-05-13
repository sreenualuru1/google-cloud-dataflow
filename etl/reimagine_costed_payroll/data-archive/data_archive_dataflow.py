import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import storage
from apache_beam.io import fileio
import re
import time
import argparse
import logging

# DoFn to match the file pattern exactly as we have scenarios like DW_JOB_D and DW_JOB_D_TL
class FilesMatchPattern(beam.DoFn):
    """
    A DoFn that filters the files based on regex and return file path attribute from FileObject
    """
    def process(self, file, pattern):
        if re.match(pattern, file.path):
            yield file.path

# This will move the files by creating one GCS client and hence faster
class MoveFiles(beam.DoFn):
    """
    A DoFn that moves files from source gcs location to target
    """
     
    def process(self, file, src_bucket_name, tgt_bucket_name, tgt_folder):  
        """
        Copies file from one gcs location to other and then deletes it 

        Args:
            file (str): Source file path
            src_bucket_name (str) : Source bucket name
            tgt_bucket_name (str) : Target bucket name
            tgt_folder (str) : Target folder path
            file_format (str) : format of the file to be archived
        """      
        gcs_path_pattern = '^gs://([^/]+)/(.*)$'
        file_name_pattern = r'[^/]+$'
        source_file_path = re.match(gcs_path_pattern, file).group(2)
        source_file_name = re.search(file_name_pattern, file).group()

        retry_count = 0
        while True:
            try:
                client=storage.Client()
                src_bucket = client.bucket(src_bucket_name)
                tgt_bucket = client.bucket(tgt_bucket_name)

                src_blob = src_bucket.blob(source_file_path)
                src_bucket.copy_blob(src_blob, tgt_bucket, tgt_folder + source_file_name )
                # Delete file from source once archived
                src_blob.delete()
                logging.info(f'{source_file_name} archived !')
                break
            except Exception as e:
                retry_count += 1
                if retry_count > 5:
                    raise Exception(f"File archival failed with : {str(e)}")
                time.sleep(30)
                continue


def log_record(record):
    logging.info(record)

def run(known_args, pipeline_args):
    """
    Runs the Beam pipeline to load data stored on GCS to BigQuery.

    Args:
        known_args : A dictionary of arguments that are known/defined in the script
        pipeline_args : A list of unknown arguments that are not defined in the script
    """
    data_file_prefix = known_args.data_file_prefix
    archive_path = known_args.archive_path
    load_date = known_args.data_load_date
    file_format = known_args.file_format

    # Setting pipeline options from arguments.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:

        # Pattern to match exact file path as we have scenarios like DW_JOB_D and DW_JOB_D_TL
        file_pattern = data_file_prefix + r"_\d+_" + f"{load_date}T" #+ date

        if file_format == 'json':
            data_file_prefix += '*.json'
        else:
            raise Exception(f'{file_format} file format not supported !!')
        
        # Bucket and path details
        pattern = '^gs://([^/]+)/(.*)$'
        src_match = re.match(pattern, data_file_prefix)
        archive_match = re.match(pattern, archive_path)

        # Archive Path - gs://archive_folder_path/table_name/YYYYMMDD/  
        archive_folder = archive_match.group(2)

        # Exract bucket name from full gcs path
        src_bucket = src_match.group(1)
        archive_bucket = archive_match.group(1)
        
        # Match the files with exact file pattern to process
        files = (
            p   | f"ListFiles" >> fileio.MatchFiles(data_file_prefix)
                | f'Reshuffle' >> beam.Reshuffle()
                | f"FilterMatchedFiles" >> beam.ParDo(FilesMatchPattern(), file_pattern)
        )

        # File Archival
        files  | f"ArchiveFiles" >> beam.ParDo(MoveFiles(), src_bucket, archive_bucket, archive_folder)

        # Logging total number of files being archived
        (
            files | "CountFiles" >> beam.combiners.Count.Globally()
                  | "LogCount" >> beam.Map(log_record)
        )

    
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_load_date', required=True, type=str)
    parser.add_argument('--data_file_prefix', required=True, type=str)
    parser.add_argument('--file_format', required=True, type=str)
    parser.add_argument('--archive_path', required=True, type=str)

    known_args, pipeline_args = parser.parse_known_args()

    config_pattern = r"^gs://.+/$"
    
    if not re.match(config_pattern, known_args.archive_path):
        raise argparse.ArgumentTypeError(f"archive_path: must follow gs://bucket_name/path_to_archive/ ")
    
    run(known_args, pipeline_args)
    
    
    