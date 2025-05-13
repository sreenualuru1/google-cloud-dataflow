import argparse
import logging
import os
import sys

import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.utils.windowed_value import WindowedValue
from google.cloud import storage
# Set up logging
logging.basicConfig(level=logging.INFO)


# Creates batches
# class GetBqInput(beam.DoFn):
#     def process(self, element, batch_size, temp_bucket_id, archive):
#         from google.cloud import bigquery
#         from google.cloud import storage

#         # Open up clients for use in processing below
#         storage_client = storage.Client()

#         # Clean up environment
#         logging.info(f"Creating clean environment")
#         clean_up(storage_client, temp_bucket_id)

#         # Gets results from query
#         client = bigquery.Client()
#         job_config = bigquery.QueryJobConfig(
#             allow_large_results=True
#         )
#         query_job = client.query(element, job_config=job_config)
#         results = query_job.result()

#         # Separate the rows into batches for processing
#         yielded_rows = {}
#         for row in results:
#             yielded_rows.update(
#                 {row['id']: {"url_rtf": row['content_attachment_url_rtf'], "data_rtf": row['content_attachment_data_rtf'], "url_pdf": row['content_attachment_url_pdf'], "data_pdf": row['content_attachment_data_pdf']}})
#             # If the list is the batch size, yield it and empty it
#             if len(yielded_rows) % int(batch_size) == 0:
#                 yield yielded_rows
#                 yielded_rows = {}

#         # Yield partially filled list for the last batch that is less than a thousand
#         logging.info(f"Separated the rows into batches for processing")
#         yield yielded_rows
#         client.close()

# ----------------- New DoFn for Batching -----------------
class BatchElementsIntoDictsFn(beam.DoFn):
    def __init__(self, batch_size):
        self.batch_size = int(batch_size)

    def start_bundle(self):
        self.buffer = {}

    def process(self, element):
        row_id = element['id']
        self.buffer[row_id] = {
            "url_rtf": element['content_attachment_url_rtf'],
            "data_rtf": element['content_attachment_data_rtf'],
            "url_pdf": element['content_attachment_url_pdf'],
            "data_pdf": element['content_attachment_data_pdf'],
        }

        if len(self.buffer) >= self.batch_size:
            #yield self.buffer
            output = self.buffer
            self.buffer = {}
            yield output 

    def finish_bundle(self):
        if self.buffer:
           # yield self.buffer
            yield beam.utils.windowed_value.WindowedValue(
                self.buffer, timestamp=0, windows=[beam.window.GlobalWindow()]
            )  
# ConvertPDF function retrieves arguments for project, dataset, document reference table and named external table
# Function then checks for 'content_attachment_url', retrieves blob, encodes in base64 string and uploads to bq column 'content_attachment_data'
class ConvertPDF(beam.DoFn):
    def process(self, element, temp_bucket_id, ignore_list):
        from google.cloud import storage
        import uuid
        import base64

        # Create a unique parquet file name
        name = uuid.uuid4()
        parquet_file_name = f'{name}.parquet'

        pylist = []

        # Open up clients for use in processing below
        storage_client = storage.Client()

        # Counter for batch numbers
        for obj in element:

            content_attachment_url = "Unassigned"

            try:
                # Setting Variables. Elements(columns) from query
                column_id = obj
                content_attachment_url_rtf = element[obj]['url_rtf']
                content_attachment_url_pdf = element[obj]['url_pdf']

                # Checking url_rtf
                if content_attachment_url_rtf is not None:
                    if content_attachment_url_rtf.startswith('https://'):
                        parsed_content_attachment_url_rtf = parse_url(content_attachment_url_rtf)
                    else:
                        parsed_content_attachment_url_rtf = content_attachment_url_rtf

                    if any(substring in parsed_content_attachment_url_rtf for substring in ignore_list):
                        logging.info(f"Skipping {parsed_content_attachment_url_rtf} as it is in the ignore list...")
                        continue
                    else:
                        content_attachment_url = parsed_content_attachment_url_rtf
                # Checking url_pdf
                elif content_attachment_url_pdf is not None:
                    if content_attachment_url_pdf.startswith('https://'):
                        parsed_content_attachment_url_pdf = parse_url(content_attachment_url_rtf)
                    else:
                        parsed_content_attachment_url_pdf = content_attachment_url_pdf

                    if any(substring in parsed_content_attachment_url_pdf for substring in ignore_list):
                        logging.info(f"Skipping {parsed_content_attachment_url_pdf} as it is in the ignore list...")
                        continue
                    else:
                        content_attachment_url = parsed_content_attachment_url_pdf

                else:
                    logging.info(f"Invalid gsutil URI format in ID {column_id}.\ncontent_attachment_url: {content_attachment_url}")
                    continue

                # Splits 'content_attachment_url' into bucket_name and blob_name
                bucket_name, blob_name = retrieve_bcket_blob(content_attachment_url)

                # Retrieves pdf blob
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_name)

                # Checks if blob exists
                if blob.exists():

                    content = ""
                    if blob.name.lower().endswith('.pdf'):
                        blob_name_pdf = blob.name.lower()
                        # Downloads blob as string
                        string_content = blob.download_as_string()

                        # Encoding PDF to base64 string
                        pdf_string_encoded = base64.b64encode(string_content)
                        content = pdf_string_encoded.decode('utf-8')

                        blob_name = blob_name_pdf

                    elif blob.name.lower().endswith('.rtf'):
                        blob_name_rtf = blob.name.lower()
                        # Donloading RTF String
                        content = blob.download_as_string()

                        blob_name = blob_name_rtf

                    else:
                        logging.info(f"Unknown Extension.")

                    size_in_bytes = sys.getsizeof(content)
                    size_in_mb = byte_to_mb(size_in_bytes, decimal_places=2)

                    set_limit = 50
                    if blob_name.endswith(".rtf") and size_in_mb < set_limit:
                        # Appending RTF string
                        pylist.append({"id": column_id, "content_attachment_data_rtf": content})
                    elif blob_name.endswith(".pdf") and size_in_mb < set_limit:
                        # Appending base64string PDF string
                        pylist.append({"id": column_id, "content_attachment_data_pdf": content})
                    else:
                        # Checking size of files
                        if size_in_mb > set_limit:
                            logging.info(f"[Failed to append] File is too large for inline representation.\n"
                                         f"id: {column_id}\nURL: {content_attachment_url}.\nInline Document Size: '{size_in_mb}' MB.")
                        else:
                            logging.info(f"[Failed to append]. Reason Unknown."
                                         f"id: {column_id}\nURL: {content_attachment_url}.\nInline Document Size: '{size_in_mb}' MB.")
                        continue

            except Exception as e:
                logging.info(
                    f"DOC-REF-ID: 19456. URI Pull or Blob Processing Failed.\nException: {e}\nID: {obj}\nURL: {content_attachment_url}")

        # Write Table to the Parquet file.
        parquet_dict = write_parquet_table(pylist, parquet_file_name)

        # Uploads Parquet to GCS bucket for external table
        upload_external_parquet(storage_client, parquet_dict, temp_bucket_id)


def byte_to_mb(size_in_bytes, decimal_places=2):
    mb = size_in_bytes / (1024 * 1024)
    size_in_mb = round(mb, decimal_places)
    return size_in_mb


def clean_up(storage_client, temp_bucket_id):
    import subprocess

    try:
        logging.info(f"Cleaning up Evironment")
        bucket_name = temp_bucket_id
        folder_name = "external_table_subfolder/"
        folder_path = f'gs://{bucket_name}/{folder_name}'

        logging.info(f"Attempting to remove files from {folder_path}")
        result = subprocess.run(
            ['gsutil', '-m', 'rm', '-r', folder_path],
            check = False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logging.info(f"File removal completed with return code: {result.returncode}")

        if result.returncode != 0:
            logging.info(f"Removal failed, checking for remaining files in {folder_path}")
            file_list = subprocess.run(
                ['gsutil', '-m', 'ls', folder_path],
                check=False,
                stdout=subprocess.PIPE
            )

            logging.info(f"File listing completed, output length: {len(file_list.stdout)} bytes")
            if file_list.returncode == 0 and file_list.stdout:
                logging.error(f"Clean up unsuccessful -- files still present")
                sys.exit(1)
            else:
                logging.info(f"Folder is empty or doesn't exit, continuing")

    except Exception as e:
        logging.info(f"DOC-REF-ID: 69513. Error during cleanup: {e}")
        sys.exit(1)


def convert_bytes_to_string(file_id):
    # Attempt to decode bytes to a UTF-8 string
    try:
        return file_id.decode('utf-8')
    except UnicodeDecodeError:
        return None


def parse_url(content_attachment_url):
    import base64
    import urllib.parse as urlparse

    try:
        # parsed the url into components
        content_attachment_url_parsed = urlparse.urlparse(content_attachment_url)
        # Extract the query string part of the url
        query = content_attachment_url_parsed.query
        # Convert the query string into a dictionary for parameter access
        query_params = urlparse.parse_qs(query)
        # Retrieve 'fieldId' from the query paramenter, defaults to None if not present
        file_id = query_params.get('fileId', [None])[0]

        if file_id:
            # If fieldId exists, decode it from base64
            return convert_bytes_to_string(base64.b64decode(file_id))
        else:
            return None

    except Exception as e:
        logging.info(f"An error occured while trying to parse the url: {e}")


def write_parquet_table(pylist, parquet_file_name, size_limit_mb=98):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import math

    try:
        schema = pa.schema([
            pa.field('id', pa.string()),
            pa.field('content_attachment_data_rtf', pa.string()),
            pa.field('content_attachment_url_rtf', pa.string()),
            pa.field('content_attachment_data_pdf', pa.string()),
            pa.field('content_attachment_url_pdf', pa.string())
        ])

        # Converts python list into table
        loaded_table = pa.Table.from_pylist(mapping=pylist, schema=schema, metadata=None)

        parquet_dict = {}
        # Calculate the size of the table
        table_size = loaded_table.nbytes / (1024 * 1024)
        if table_size <= size_limit_mb:
            with pq.ParquetWriter(parquet_file_name, loaded_table.schema) as writer:
                # Write the entire table if it's within the size limit
                writer.write_table(loaded_table)
                table_size = get_filesize(parquet_file_name)
                parquet_dict[parquet_file_name] = table_size
                writer.close()
        else:
            # Split the table into smaller tables and write each to a separate file
            rows = len(loaded_table)
            split_size = int(rows * (size_limit_mb / table_size))

            for i in range(0, rows, split_size):
                split_table = loaded_table.slice(i, split_size)
                split_file_name = f"{os.path.splitext(parquet_file_name)[0]}_part{math.floor(i // split_size)}.parquet"
                with pq.ParquetWriter(split_file_name, split_table.schema) as writer:
                    writer.write_table(split_table)
                    split_file_size = get_filesize(split_file_name)
                    parquet_dict[split_file_name] = split_file_size
                    writer.close()

        return parquet_dict

    except Exception as e:
        logging.info(f"DOC-REF-ID: 95596. An error occured while writing to parquet table: {e}")


def get_filesize(split_file_name):
    file_size = os.path.getsize(split_file_name)
    size = file_size / (1024 * 1024)
    return size


def retrieve_bcket_blob(content_attachment_url):
    try:
        path_parts = content_attachment_url[5:].split('/', 1)
        bucket_name = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ''
        return bucket_name, blob_name
    except Exception as e:
        logging.info(f"An error occured while trying split and retrieve the bucket and blob: {e}")


def upload_external_parquet(storage_client, parquet_dict, temp_bucket_id):
    for parquet_file_name in parquet_dict:

        if not os.path.isfile(parquet_file_name):
            logging.info(f"The specified Parquet file does not exist.")
            raise ValueError("The specified Parquet file does not exist.")
        try:
            dest_bucket = storage_client.bucket(temp_bucket_id)
            dest_blob = dest_bucket.blob(f'external_table_subfolder/{parquet_file_name}')
            dest_blob.upload_from_filename(parquet_file_name)

            try:
                os.remove(parquet_file_name)
            except FileNotFoundError:
                logging.info(f"Parquet File {parquet_file_name} does not exist")

        except Exception as e:
            logging.info(f"DOC-REF-ID: 25750. An error occurred while trying upload the parquet to the bucket: {e}")


# Runs dataflow job
def run_dataflow_job(argv=None, save_main_session=True):
    # parsing arguments project_id, dataset_id, target_table_id,base_table_id and external_table_id
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--intfhr_project_id',
        dest='intfhr_project_id',
        required=True,
        help='Name of Project')
    parser.add_argument(
        '--target_dataset_id',
        dest='target_dataset_id',
        required=True,
        help='Name of Dataset')
    parser.add_argument(
        '--base_table_id',
        dest='base_table_id',
        required=True,
        help='Name of Primary Base table')
    parser.add_argument(
        '--target_table_id',
        dest='target_table_id',
        required=True,
        help='Name of Primary Target table')
    parser.add_argument(
        '--batch_size',
        dest='batch_size',
        required=True,
        help='Number of files to process ata a time.')
    parser.add_argument(
        '--temp_bucket_id',
        dest='temp_bucket_id',
        required=True,
        help='Temp bucket ID.')
    parser.add_argument(
        '--ignore_list',
        dest='ignore_list',
        required=True,
        help='The ignore_list of LPR Datasets')
    parser.add_argument(
        '--archive',
        dest='archive',
        required=True,
        help='Archiving the folders')
    parser.add_argument(
        '--start_date',
        dest='start_date',
        required=True,
        help='start_date: yyyy-mm-dd ex. 2024-01-01')
    parser.add_argument(
        '--end_date',
        dest='end_date',
        required=True,
        help='end_date: yyyy-mm-dd ex. 2024-01-01')
    parser.add_argument(
        '--date_type',
        dest='date_type',
        required=True,
        help='date_type: last_udpated or date')

    known_args, pipeline_args = parser.parse_known_args(argv)
    beam_options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    beam_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True

    # Creates a query to extract and read our data from bigquery
    def read_from_bigquery():
        logging.info(f"Starting Read From BigQuery")

        start_date = known_args.start_date
        end_date = known_args.end_date
        date_type = known_args.date_type

        if known_args.date_type == 'default':
            dt = 'dr.last_updated'
        else:
            dt = f"dr.{date_type}"

        if known_args.start_date == 'default':
            dates = "DATE_SUB(current_date(), INTERVAL 2 DAY) AND current_date()"
        else:
            dates = f"DATE('{start_date}') AND DATE('{end_date}')"

        query = f"""
            SELECT
              dr.id,
              dr.content_attachment_url_rtf,
              dr.content_attachment_data_rtf,
              dr.content_attachment_url_pdf,
              dr.content_attachment_data_pdf
            FROM
              `{known_args.intfhr_project_id}.{known_args.target_dataset_id}.{known_args.base_table_id}` AS dr
            LEFT JOIN
          `{known_args.intfhr_project_id}.{known_args.target_dataset_id}.{known_args.target_table_id}` AS ic
            ON
          	dr.id = ic.id
            WHERE
              DATE({dt}) BETWEEN {dates}
              AND
                (dr.content_attachment_data_rtf IS NULL AND dr.content_attachment_data_pdf IS NULL)
              AND
                (dr.content_attachment_url_rtf IS NOT NULL OR dr.content_attachment_url_pdf IS NOT NULL)
              AND ic.id IS NULL
            ORDER BY dr.document_creation_date;
            """

        return query

    try: #

        ignore_list = known_args.ignore_list.split(',')
        clean_up(storage.Client(), known_args.temp_bucket_id)
        # Our Jobs in Dataflow
        with beam.Pipeline(options=beam_options) as p:
            # (p
            #  | 'Starting Job 1' >> beam.Create([read_from_bigquery()])
            #  | 'Get source data and split it into batches' >> beam.ParDo(GetBqInput(),
            #                                                              known_args.batch_size,
            #                                                              known_args.temp_bucket_id,
            #                                                              known_args.archive)
            #  | "Reshuffle" >> beam.Reshuffle()
            #  | 'Extract PDF, Convert to base64, Upload parquet into GCS' >> beam.ParDo(ConvertPDF(),
            #                                                                            known_args.temp_bucket_id,
            #                                                                            ignore_list)
             
            #  )
            (
                p
                | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
                    query=read_from_bigquery(),
                    use_standard_sql=True)
                | 'Batch elements into dicts' >> beam.ParDo(BatchElementsIntoDictsFn(known_args.batch_size))
                | "Reshuffle" >> beam.Reshuffle()
                | 'Convert to Base64 and Upload' >> beam.ParDo(ConvertPDF(),
                                                               known_args.temp_bucket_id,
                                                               ignore_list)
            )


    except Exception as e:
        print("Pipeline Failed")
        logging.info(f"Pipeline Failed: An error occured: {e}")
        print(f"An error occurred: {e}", file=sys.stderr)

    print("Pipeline Finishing...")
    logging.info("Pipeline Finishing...")


if __name__ == "__main__":
    print("Starting the Program")
    run_dataflow_job()
    print("Program Finished")
