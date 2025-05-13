"""
This script refreshes the following storage buckets for the harwick incremental and deposits them in an Archive. 
1. Zipped Bucket
2. Unzipped Bucket
Notes on the required arguments. 
    {} denotes the argument requirements
    1. Location of the Zipped Bucket
        gs://{zipped_bucket}/{zip_prefix}/{extract}*.JSON'
    2. Location of the Unzipped Bucket
        gs://{unzipped_bucket}/{unzip_prefix}/
    3. Location of the Archive Bucket
        date_achive_prefix = {archive_prefix} + date of the DATA status JSON in the Zipped Bucket
        gs://{unzipped_bucket}/date_archive_prefix/
    4. Location of the External Table Bucket
        gs://{unzipped_bucket}/{bicc_external_prefix}/
--------------------------MAJOR CHANGE LOG--------------------------
Version 1.0.0
Description: Original Harwick-Ofusion Incremental Archive Job
Date: 2024-11-13
Author: Zach Montoya
"""

import apache_beam as beam
import logging
import argparse
import time

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.cloud import storage

def DateTimeFormat(today):
    '''
    Used to generate folder name for the Archive.
    If the execution time is between 0000 and 1800 the previous day will be used.
    '''
    localized_time = today.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo('America/Chicago'))
    
    if 0 <= localized_time.hour <= 18: # 24 hour format
        date_object = localized_time - timedelta(days=1)
    else:
        date_object = localized_time
    
    # Format the date as YYYY-MM-DD
    formatted_date = date_object.strftime("%Y-%m-%d")
    
    return formatted_date

class LogIngestedFiles:
    '''
    Converting ingested match file objects to generator and logs.
    Args:   Element: FileMetadata object
    Yields: Element: Generator 
    '''
    def process(self, element):
        logging.info(f"\nPIPELINE INFO: ********* Starting Pipeline Stage 2: Logging Ingested Files  ********* ")
        logging.info(f"PIPELINE INFO: GSC file ingested: {element}")
        return element

class MoveFilesWithinGCS(beam.DoFn):
    '''
    Copies matched file generators from the zipped location to the archive location.
    Deletes matched files in the unzipped location, external table locaiton, and zipped location.
    Args:   Element: Generator
            Str: zip_bucket
            Str: zip_prefix
            Str: extract
            Str: unzipped_bucket
            Str: unzip_prefix
            Str: archive_bucket
            Str: archive_prefix
            Str: bicc_external_prefix
            Str: dated_runtime
    Yields: Element: Generator 
    '''
    
    def process(self, element, zip_bucket, zip_prefix, extract, unzipped_bucket, unzip_prefix, archive_bucket, archive_prefix, bicc_external_prefix, dated_runtime):
                
        logging.info(f"\nPIPELINE INFO: ********* Starting Pipeline Stage 3: Move files within GCS  ********* ")
        
        dated_archive_prefix = self.CreatedDatedArchivedPrefix(element, archive_prefix, extract, dated_runtime, archive_bucket)
        
        zip_blob_names = self.ArchiveZip(element, zip_bucket, zip_prefix, extract, archive_bucket, dated_archive_prefix)
        
        self.ClearZippedAndUnzippedBuckets(element, zip_bucket, zip_prefix, zip_blob_names, unzipped_bucket, unzip_prefix, archive_bucket, dated_archive_prefix, extract, bicc_external_prefix)

    def CheckPrefixExists(self, element, destination_bucket, new_archive_prefix):
    
        # Checking if the archive prefix exists but grabbing the FIRST blob and converting to a list
        blobs = list(destination_bucket.list_blobs(prefix=new_archive_prefix, max_results=1)) # max_results = 1 is important
        
        # if the list is empty the archive location is avaliable for use
        if len(blobs) > 0 :
            
            # result returned from list blobs -> prefix is used
            prefix_used = True 
            for blob in blobs:
                
                # Extracting the revision letter of the dated prefix (ie. 2025-01-22-A), 
                blob_name = str(blob.name)
                folder_name = blob_name.rsplit('/', maxsplit=1)
                archive_revision_rsplit = folder_name[0].rsplit('-',maxsplit=1)
                
                archive_revision_letter = archive_revision_rsplit[1]
                archive_revision_base = archive_revision_rsplit[0]
                
                # if it is just (2025-01-22) then assigns a letter A
                try: 
                    int(archive_revision_letter)
                    new_archive_prefix = f"{new_archive_prefix}-A"

                # if it is infact a letter it will incremented then the base will be used.
                except:
                    next_archive_revision_letter = chr(ord(archive_revision_letter) + 1)
                    new_archive_prefix = f"{archive_revision_base}-{next_archive_revision_letter}"
            
        else:
            prefix_used = False
        
        return prefix_used, new_archive_prefix
    
    def CreatedDatedArchivedPrefix(self, element, archive_prefix, extract, dated_runtime, archive_bucket):
        try:
            # Initialize GCS client
            client = storage.Client()

            # Get source and destination buckets
            destination_bucket = client.bucket(archive_bucket)
                
            logging.info(f"PIPLINE INFO: Date Obtained from Extract: {dated_runtime}")
            
            # Formatting the extract to match the mapper style
            extract_mapper_format = extract.replace("_","-",1).lower()

            # Returning new prefix
            new_archive_prefix = f"{archive_prefix}/{extract_mapper_format}/{dated_runtime}"
            
            # Recursively testing if prefix is used
            prefix_used = True
            while prefix_used:
                prefix_used, new_archive_prefix = self.CheckPrefixExists(element, destination_bucket, new_archive_prefix)
                
            logging.info(f"PIPLINE INFO: New archive prefix created: {new_archive_prefix}")
            
            return new_archive_prefix
        
        finally:
           client.close()
        
    def ArchiveZip(self, element, zip_bucket, zip_prefix, extract, archive_bucket, dated_archive_prefix):
        try:
            # Initialize GCS client
            client = storage.Client()

            # Get source and destination buckets
            source_bucket = client.bucket(zip_bucket)
            destination_bucket = client.bucket(archive_bucket)

            # List files in drop-in folder ( source folder)
            source_prefix = f"{zip_prefix}/{extract}"
            zipblobs = list(source_bucket.list_blobs(prefix=source_prefix))

            # Copy each file from source to destination folder
            logging.info(f"PIPELINE INFO: Starting Archive \tFrom: gs://{zip_bucket}/{source_prefix}* \tTo: gs://{archive_bucket}/{dated_archive_prefix}\n")
            for blob in zipblobs:
                if not blob.name.endswith('/'): #Omit Folders or files that end with /
                    name_split = blob.name.rsplit("/", 1)
                    new_blob = source_bucket.blob(blob.name.replace(name_split[0], dated_archive_prefix))
                    
                    # retry counter
                    retry_counter = 0
                    
                    while retry_counter < 5:
                        try:
                            logging.info(f"PIPELINE INFO: Attempt #{retry_counter} Attempting to Archived File: {name_split[-1]}")
                            source_bucket.copy_blob(blob, destination_bucket, new_blob.name)
                            break
                        except:
                            wait_time = 2 ** retry_counter
                            logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to delete recently Archived Zipped Blob in gs://{name_split[-1]}")                  
                            time.sleep(wait_time)
                            
                            retry_counter += 1
                            
                    if retry_counter >= 5 :
                           raise Exception(f"PIPELINE ERROR CODE HBIA2: FAILED to delete recently Archived Zipped Blob in gs://{name_split[-1]} after #{retry_counter} attempts")                  
                else:
                    pass
            logging.info(f"PIPELINE INFO: ********* Successfully Archived All Files ********* \n")
            
            return zipblobs
        
        except Exception as e:
                logging.error(f"PIPELINE ERROR CODE HBIA2: Error Copying files from Zipped Location to Archive Location, Please Investigate :- {e}")
                
        finally:
           client.close()

    def ClearZippedAndUnzippedBuckets(self, element, zip_bucket, zip_prefix, zip_blob_names, unzipped_bucket, unzip_prefix, archive_bucket, dated_archive_prefix, extract, bicc_external_prefix):
        try:
            # Initialize GCS client
            client = storage.Client()

            # Get client connections for the three buckets
            unzip_bucket_client = client.bucket(unzipped_bucket)
            archive_bucket_client = client.bucket(archive_bucket)

            # Obtain blobs archived        
            archiveblobs = list(archive_bucket_client.list_blobs(prefix=f"{dated_archive_prefix}/{extract}"))

            # Checking the blobs archived correctly before deleting
            if len(archiveblobs) == len(zip_blob_names):
                
                # **************************
                # BICC ZIPPED DELETION
                # **************************                
                # # Delete all source files that were just copied
                logging.info(f"PIPELINE INFO: Attempting to delete Recently Archived Zipped Blobs in gs://{zip_bucket}/{zip_prefix}/{extract}*")
                
                for zip_blob in zip_blob_names:
                    if not zip_blob.name.endswith('/'): # Ignorning folders
                        bicc_zipped_retry_counter = 0
                        
                        # bicc-zipped blob deletion with retries
                        while bicc_zipped_retry_counter < 5:
                            try:
                                    
                                logging.info(f"PIPELINE INFO: Attempt #{bicc_zipped_retry_counter} Trying to delete recently Archived Zipped Blob in gs://{zip_bucket}/{zip_blob.name}")
                                zip_blob.delete()
                                break
                            
                            except:
                                wait_time = 2 ** bicc_zipped_retry_counter
                                logging.warning(f"PIPELINE WARNING: Attempt #{bicc_zipped_retry_counter} failed RETRYING in {wait_time}s to delete recently Archived Zipped Blob in gs://{zip_bucket}/{zip_blob.name}") 
                                time.sleep(wait_time)
                                
                                bicc_zipped_retry_counter += 1
                        
                        if bicc_zipped_retry_counter >= 5 :
                            raise Exception(f"PIPELINE ERROR CODE HBIA3: FAILED to delete recently Archived Zipped Blob in gs://{zip_bucket}/{zip_blob.name} after #{bicc_zipped_retry_counter} attempts")
                                
                logging.info(f"PIPELINE INFO: Successfully deleted ALL Recently Archived Zipped Blobs in gs://{zip_bucket}/{zip_prefix}/{extract}*\n")

                # **************************
                # BICC UNZIPPED DELETION
                # **************************
                # List and Delete files in unzipped folder
                logging.info(f"PIPELINE INFO: Attempting to delete Unzipped Blobs in gs://{unzipped_bucket}/{unzip_prefix}/*")
                unzipblobs = list(unzip_bucket_client.list_blobs(prefix=unzip_prefix))
                
                for unzip_blob in unzipblobs:
                    if not unzip_blob.name.endswith('/'): # Ignorning folders
                        bicc_unzipped_retry_counter = 0
                        
                        # bicc-zipped blob deletion with retries
                        while bicc_unzipped_retry_counter < 5:
                            try:
                                    
                                logging.info(f"PIPELINE INFO: Attempt #{bicc_unzipped_retry_counter} Trying to delete Unzipped Blob in gs://{unzipped_bucket}/{unzip_blob.name}")
                                unzip_blob.delete()
                                break
                            
                            except:
                                wait_time = 2 ** bicc_unzipped_retry_counter
                                logging.warning(f"PIPELINE WARNING: Attempt #{bicc_unzipped_retry_counter} failed RETRYING in {wait_time}s to delete Unzipped Blob in gs://{unzipped_bucket}/{unzip_blob.name}")                                 
                                time.sleep(wait_time)
                                
                                bicc_unzipped_retry_counter += 1
                        
                        if bicc_unzipped_retry_counter >= 5 :
                            raise Exception(f"FAILED to delete Unzipped Blob in gs://{unzipped_bucket}/{unzip_blob.name} after #{bicc_unzipped_retry_counter} attempts")

                logging.info(f"PIPELINE INFO: Successfully deleted ALL Unzipped Blobs in gs://{unzipped_bucket}/{unzip_prefix}/*\n")
                
                # **************************
                # BICC EXTERNAL DELETION
                # **************************                      
                # Listing external blobs then deleting
                external_extract_prefix = f"{bicc_external_prefix}"
                logging.info(f"PIPELINE INFO: Attempting to delete External Table Blobs in in gs://{unzipped_bucket}/{external_extract_prefix}/*")
                external_files = list(unzip_bucket_client.list_blobs(prefix=external_extract_prefix))
                
                for external_file_blob in external_files:
                    if not external_file_blob.name.endswith('/'): # Ignorning folders

                        external_file_retry_counter = 0
                        
                        while external_file_retry_counter < 5:
                            try:
                                    
                                logging.info(f"PIPELINE INFO: Attempt #{external_file_retry_counter} Trying to delete External Table Blob in gs://{unzipped_bucket}/{external_file_blob.name}")
                                external_file_blob.delete()
                                break
                            
                            except:
                                wait_time = 2 ** external_file_retry_counter
                                logging.warning(f"PIPELINE WARNING: Attempt #{external_file_retry_counter} failed RETRYING in {wait_time}s to delete External Table Blob in gs://{unzipped_bucket}/{external_file_blob.name}")                                                              
                                time.sleep(wait_time)
                                
                                external_file_retry_counter += 1
                        
                        if external_file_retry_counter >= 5 :
                            raise Exception(f"PIPELINE ERROR CODE HBIA3: FAILED to delete External Table Blob in gs://{unzipped_bucket}/{external_file_blob.name} after #{external_file_retry_counter} attempts")
                logging.info(f"PIPELINE INFO: Successfully deleted ALL External Table Blobs in in gs://{unzipped_bucket}/{external_extract_prefix}/*\n")
                

            else:
                raise Exception("PIPELINE ERROR CODE HBIA3: Error archiving. Count of Archived Location Files does not match count of Zipped Location Files")

        except Exception as e:
            logging.error(f"PIPELINE CODE HBIA4: Error Clearing files from Zipped Location, Unzipped Location and External Table Location, Please Investigate :- {e}") 
        
        finally:
           client.close()

def run_pipeline(zip_bucket, zip_prefix, extract, unzipped_bucket, unzip_prefix, archive_bucket, archive_prefix, bicc_external_prefix):

    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    
    logging.info(f"PIPELINE INFO: ##################### Initiating Archive Pipeline  ##################### \nwith parameters:\n--zip_bucket={zip_bucket}\n--zip_prefix={zip_prefix}\n--extract={extract}\n--unzipped_bucket={unzipped_bucket}\n--unzip_prefix={unzip_prefix}\n--archive_bucket={archive_bucket}\n--archive_prefix={archive_prefix}\n--bicc_external_prefix={bicc_external_prefix}\n")

    dated_runtime = DateTimeFormat(datetime.now(timezone.utc))

    logging.info("\nPIPELINE INFO: ********* Starting Pipeline Stage 1 Starting: List Files *********")
    with beam.Pipeline(options=options) as p:
        _ = (p | "List Files" >> fileio.MatchFiles(f'gs://{zip_bucket}/{zip_prefix}/*{extract}*STATUS_DATA*.JSON',  empty_match_treatment=fileio.EmptyMatchTreatment.DISALLOW)
            | 'Logging Ingested Files' >> beam.Map(LogIngestedFiles().process)
            | "Move files within GCS" >> beam.ParDo(MoveFilesWithinGCS(), zip_bucket, zip_prefix, extract, unzipped_bucket, unzip_prefix, archive_bucket, archive_prefix, bicc_external_prefix, dated_runtime) #, unzip_prefix, archive_bucket, archive_prefix, zip_bucket, zip_prefix))
        )
    logging.info("PIPELINE INFO: ##################### Completed Archive Pipeline #####################")

if __name__ == '__main__':
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        logging.info("PIPELINE INFO: HELLO STARTING THE INC PIPELINE ARCHIVE JOB")

        try:
               
                parser = argparse.ArgumentParser(description='Data Archive DataFlow program')
                parser.add_argument('--zip_bucket', type=str, required=True, help='Name of the GCS bucket where JSON and Mapping file is stored')
                parser.add_argument('--zip_prefix', type=str, required=True, help='GCS Bucket Folder(prefix) for source zip Files dumped from S3 bucket- Example: zip_prefix/zip or any other subofolder in the bucket')
                parser.add_argument('--extract_name', type=str, required=True, help='Name of the extract')
                parser.add_argument('--unzipped_bucket', type=str, required=True, help='Name of the GCS bucket where JSON and Mapping file is stored')
                parser.add_argument('--unzip_prefix', type=str, required=True, help='GCS Bucket Folder(prefix) Containing PVO Files after unzipping files - Example: bicc_zipped/_MCC or any other subofolder in the bucket')
                parser.add_argument('--archive_bucket', type=str, required=True, help='Name of the GCS bucket where the data will be archived')
                parser.add_argument('--archive_prefix', type=str, required=True, help='GCS Bucket Folder(prefix) target folder that will be containing archived zip files - Example: archive_prefix/zip or any other subofolder in the bucket')
                parser.add_argument('--bicc_external_prefix', type=str, required=True, help='GCS Bucket Folder(prefix) target folder that will be containing .csv and .pecsv files for the external tables - Example: external-tables or any other subfolder in the bucket')

                args,pipeline_args = parser.parse_known_args()
                
                # Checking the pipeline arguments before execution
                if "/" in args.zip_bucket:
                    raise argparse.ArgumentTypeError(f"--zip_bucket: {args.zip_bucket} contains '/' or subpath must be top level storage bucket")
                if "/" in args.unzipped_bucket:
                    raise argparse.ArgumentTypeError(f"--unzipped_bucket: {args.unzipped_bucket} contains '/' or subpath must be top level storage bucket")
                if args.zip_prefix.startswith("/") or args.zip_prefix.endswith("/"):
                    raise argparse.ArgumentTypeError(f"--zip_prefix: {args.zip_prefix} starts or ends with '/'. May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
                if args.unzip_prefix.startswith("/") or args.unzip_prefix.endswith("/"):
                    raise argparse.ArgumentTypeError(f"--unzip_prefix: {args.unzip_prefix} starts or ends with '/'. May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
                if args.archive_prefix.startswith("/") or args.archive_prefix.endswith("/"):
                    raise argparse.ArgumentTypeError(f"--archive_prefix: {args.archive_prefix} starts or ends with '/'. May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
                if args.bicc_external_prefix.startswith("/") or args.bicc_external_prefix.endswith("/"):
                    raise argparse.ArgumentTypeError(f"--bicc_external_prefix: {args.bicc_external_prefix} starts or ends with '/'. May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
                if args.extract_name.startswith("/") or args.extract_name.endswith("/"):
                    raise argparse.ArgumentTypeError(f"--extract_name: {args.extract_name} starts or ends with '/'. The extract name should be prefix of the extract (eg. FSC_LONG5 )")
                if "." in args.extract_name:
                    raise argparse.ArgumentTypeError(f"--extract_name: {args.extract_name} Contains a period '.' The extract name should be prefix of the extract (eg. FSC_LONG5 )")
                
                run_pipeline(args.zip_bucket, args.zip_prefix, args.extract_name, args.unzipped_bucket, args.unzip_prefix, args.archive_bucket, args.archive_prefix, args.bicc_external_prefix)
                
        except fileio.filesystem.BeamIOError as e:
            logging.error(f"PIPELINE ERROR CODE HBIA1: PIPELINE STAGE 1 'List Files'. {e}") 

        except Exception as e:
            raise TypeError(f"PIPELINE FATAL ERROR CODE HBIA99: in __main__ , Please Investigate :- {e}") 
