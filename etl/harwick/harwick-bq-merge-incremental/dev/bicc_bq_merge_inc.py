"""
This script is used in the Harwick Incremental Extract process to execute the following actions on a complete series of unzipped files. Execution is parallelized at the table level.
1. Check .mdcsv schema is the same in bicc_zipped as the base table
2. Copy .csv & .pecsv to bicc_external (extract_specific folders/tablespecific_subfolder) from bicc_unzipped
3. Dynamically create merge sql and execute
4. Clean up bicc_external

Notes on the required arguments. 
    A. Location of the unzipped schema files (.mdcsv), primary key files (.pecsv), and incremental data files (.csv).
        gs://{unzipped_bucket}/{unzipped_prefix}/*.mdcsv'
        gs://{unzipped_bucket}/{unzipped_prefix}/*.pecsv'
        gs://{unzipped_bucket}/{unzipped_prefix}/*.csv'
    B. Location of Mapper CSV File
        gs://{flex_template_bucket}/{csv_prefix}/{csv_map_file}
    C. Location of the bicc_external bucket
        gs://{unzipped_bucket}/{bicc_external}/{extract}/inc_table_name
        gs://{unzipped_bucket}/{bicc_external}/{extract}/pk_table_name
        inc_tablename - derived from mapper file
        pk_tablename - derived from mapper file
    D. Location of Dump Tables
        {bq_project}.{bq_dump_dataset}.inc_tablename
        {bq_project}.{bq_dump_dataset}.pk_tablename
        tablename - derived from mapper file
    E. Location of Base Table
        {bq_project}.{bq_basetable_source_dataset}.tablename
        tablename - derived from mapper file
        
--------------------------MAJOR CHANGE LOG--------------------------
Version 1.0.0
Description: Original Harwick-Ofusion Incremental Merge Job
Date: 2024-11-13
Author: Zach Montoya, Sheikh Fahad, Raphael Oliveira        
"""
import argparse
import apache_beam as beam
import csv
import logging


from datetime import datetime, timezone
from apache_beam.io import fileio
from google.cloud import storage
from google.cloud import bigquery
from collections import deque
from io import StringIO

def oracle_to_bigquery_datatype(oracle_datatype, col_precision=None):
    """
    Helper function to convert Oracle datatype to BQ datatype from a dictionary
    Args: oracle_datatype: oracle datatype
    Returns: string: BQ datatype
    """
    conversion_dict = {
        'VARCHAR': 'STRING',
        'NVARCHAR': 'STRING',
        'VARCHAR2': 'STRING',
        'NVARCHAR2': 'STRING',
        'CHAR': 'STRING',
        'NCHAR': 'STRING',
        'FLOAT': 'FLOAT64',
        'INTEGER': 'INT64',
        'SHORTINTEGER': 'INT64',
        'LONGINTEGER': 'INT64',
        'NUMBER': 'NUMERIC',
        'LONG': 'BYTES',
        'DATE': 'DATE',
        'BINARY_DOUBLE': 'FLOAT64',
        'BINARY_FLOAT': 'FLOAT64',
        'BLOB': 'BYTES',
        'BFILE': 'STRING',
        'DATETIME': 'DATETIME',
        'TIMESTAMP': 'DATETIME',
        'NUMERIC':{
            '18':'INT64',
            'Else':'BIGNUMERIC'},
        'CLOB': 'STRING'
    }
    
    if col_precision is None: 
        return conversion_dict.get(oracle_datatype)
    else:
        return conversion_dict.get(oracle_datatype)[col_precision]

class IngestSchemaFiles(beam.DoFn):
    '''
    Creating a generator per elements to hydrate throught the process based on the list files.
    Args:       element: listfiles match
    Yields:    element: generator
    '''
    def process(self, element):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 2 Starting: Logging Ingested Files *********")
        logging.info(f"PIPELINE INFO: GCS file ingested: {element}")
        file_name = element.path.split("/")[-1]
        pvo_name = file_name.split("-")[0]
        data_element = {
            'FILE_NAME': str(pvo_name), #PVO
            'FILE_PATH': element.path
        }
        yield data_element

class CheckMapperCSV(beam.DoFn):
    '''
    Reads a mapper CSV file that relates PVO name to base table names, incremental table names, and primary key table names. 
    Args:       Element: Generator
                Str: csv_map_bucket
                Str: csv_prefix
                Str: csv_map_file
          
    Yields:    Element: Hydrated Generator
    '''
    def process(self, element, csv_map_bucket ,csv_prefix, csv_map_file):
        logging.basicConfig(level=logging.INFO)

        try:
            logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 3 Starting: Reading the MapperCSV *********")
            
            # Connecting to GCS bucket to read the Mapper File
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(csv_map_bucket)
            blob = bucket.get_blob(f'{csv_prefix}/{csv_map_file}')

            # Reading the Mapper File
            blob = blob.download_as_text()
            rows = csv.reader(blob.splitlines())
            
            # Set flag for names not found in CSV & adding the Table Names to the generators. 
            not_found = True
            for row in rows:
                if element.get('FILE_NAME').lower() == row[0]:
                    not_found = False
                    
                    # If the FILE_NAME is found, add TABLENAMEs and EXTRACT to the generator elements
                    data_elements = {
                        'FILE_NAME': element.get('FILE_NAME'), #PVO
                        'FILE_PATH': element.get('FILE_PATH'),
                        'EXTRACT':row[3],
                        'BASETABLE': row[1],
                        'PKTABLE': row[7],
                        'INCTABLE': row[8]
                    }
                    logging.info(f"PIPELINE INFO: Successfully extracted elements from a CSV Mapper File for Table: {row[1]}")
                    yield data_elements
                    break

            if not_found:
                # If the PVO_NAME is not found, add message to row
                data_elements = {
                    'FILE_NAME': element.get('FILE_NAME'), 
                    'FILE_PATH': element.get('FILE_PATH'), 
                    'EXTRACT':"PVO NOT IN MAPPER",
                    'BASETABLE': "PVO NOT IN MAPPER",
                    'PKTABLE': "PVO NOT IN MAPPER",
                    'INCTABLE': "PVO NOT IN MAPPER",
                }
                logging.error(f"PIPELINE ERROR CODE HBIM2: Elements missing from the CSV Mapper File: {data_elements}")
                yield data_elements

            storage_client.close()

        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIM3: Error reading the CSV Mapper file. ERROR :- ", {e})

class ParseSchema(beam.DoFn):
    '''
    Used to Parse the mdcsv schema file to validate the schema in the schema file is the same as the base table
    and to generate a schema_dict which is an input to the generate SQL function.
    Args:       Element: Generator
                Str: unzipped_bucket
                Str: bq_project
                Str: base_dataset
                
    Yields:    Element: Hydrated Generator
    '''
    def process(self, element, unzipped_bucket, bq_project, base_dataset, dump_dataset):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 4 Starting: ParseSchema *********")

        # Obtaining the .mdcsv prefix
        file_path = element.get('FILE_PATH')
        file_path_split = file_path.split('/')
        mdcsv_prefix = '/'.join(file_path_split[3:])

        # Table Names
        base_table_name = element.get("BASETABLE")
        inc_table_name = element.get("INCTABLE")
        pk_table_name = element.get("PKTABLE")

        # Parsing the schema file to yield a dictionary
        element_schema_dict = self.parse_schema_file(unzipped_bucket, mdcsv_prefix, base_table_name, inc_table_name, pk_table_name)
        
        # Checking the schema of the base table
        existing_table_schema = self.query_table_schema(bq_project, base_dataset, base_table_name)
        
        # Obtaining the schema ordered by ordinal position of the data external table and primary key external table:
        external_data_table_schema_query = self.query_table_schema(bq_project, dump_dataset, inc_table_name)
        external_pk_table_schema_query = self.query_table_schema(bq_project, dump_dataset, pk_table_name)

        # Converting the result of the ordinal position query to a list of the header names
        external_data_table_schema = [row[0].upper() for row in external_data_table_schema_query]
        external_pk_table_schema = [row[0].upper() for row in external_pk_table_schema_query]
        
        logging.info(f"PIPELINE INFO: Schema Query executed on basetable {bq_project}.{base_dataset}.{base_table_name}")
        
        # Obtaining the schema_validator from the element schema dict
        schema_file_schema = element_schema_dict.get(inc_table_name).get('schema_validator')

        # Comparing the schema_validator from the mdcsv file and the query from the base_table. Returns True if they are equal. 
        # Hydrating the generator elements to include the element schema dict for the query generation.
        compare_schema_status, element_schema_list =  self.compare_schema_files(element.get('FILE_NAME'), existing_table_schema, schema_file_schema) 
        if compare_schema_status:
            data_elements = {
                'FILE_NAME': element.get('FILE_NAME'), #PVO
                'FILE_PATH': element.get('FILE_PATH'),
                'EXTRACT': element.get('EXTRACT'),
                'BASETABLE': element.get('BASETABLE'),
                'PKTABLE': element.get('PKTABLE'),
                'INCTABLE': element.get('INCTABLE'),
                'ELEMENT_SCHEMA_DICT': element_schema_dict,
                'ELEMENT_SCHEMA_LIST': element_schema_list,
                'EXT_DATA_TABLE_SCHEMA': external_data_table_schema,
                'EXT_PK_TABLE_SCHEMA': external_pk_table_schema
            }
            logging.info(f"PIPELINE INFO: Successfully parsed the Schema File for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')}")
            yield data_elements
        
        else:
            logging.error(f"PIPELINE ERROR: Unable to parse the Schema File for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')}")

    def parse_schema_file(element, unzipped_bucket, mdcsv_prefix, base_table_name, inc_table_name, pk_table_name):
        """
        Parses the mdcsv files in the bucket
        Args:       schema_file: a list of schema file(s)
        Returns:    dict: Returns a dict with extract name, column names, and column datatypes
        """
        try:
            # Connecting to GCS Bucket
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(unzipped_bucket)

            # Connecting to mdcsv blob
            blob = bucket.get_blob(f'{mdcsv_prefix}')
            blob = blob.download_as_text()
            rows = blob.splitlines()

            schema_dict = {}

            for line in rows[1:]:  # skip header
                
                # Extract Schema Parts
                parts = line.strip().split('|||')
                column_name = parts[7]
                oracle_datatype = parts[9]
                pk = parts[-2]  # 'PK' or ''
                col_precision = str(parts[12])
                
                # Convert Oracle datatype to BigQuery Datatype
                if col_precision == '18' and oracle_datatype == 'NUMERIC':    
                    bigquery_datatype = oracle_to_bigquery_datatype(oracle_datatype, col_precision)
                elif oracle_datatype == 'NUMERIC' and col_precision != '18':
                    col_precision = 'Else'      
                    bigquery_datatype = oracle_to_bigquery_datatype(oracle_datatype, col_precision)    
                else:
                    bigquery_datatype = oracle_to_bigquery_datatype(oracle_datatype)
                    

                # Add table:{k:v} to schema_dict if table_name not already present
                if inc_table_name not in schema_dict:
                    schema_dict[inc_table_name] = {
                        # 'create_table_body': [],
                        'base_table_name': [base_table_name],
                        'pk_table_name': [pk_table_name],
                        # 'create_pk_table_body': [],
                        'schema_validator': [], # Use to compare mdcsv generated schema to BQ information schema
                        'merge_pk': [],  # Use in CLUSTER BY, QAULIFY...PARTITION BY, MERGE ON, - MERGE SCRIPT
                        'merge_when_matched': [],  # Use in WHEN MATCHED AND () - MERGE SCRIPT
                        'merge_update_set': [],  # Use in UPDATE SET - MERGE SCRIPT
                        'merge_insert': [],  # Use in INSERT @columns VALUES ... - MERGE SCRIPT
                        'merge_insert_source': [],  # Use in INSERT ... VALUES s.@columns - MERGE SCRIPT
                        'delete_where_not_exists': [], # Use in delete where not exists, WHERE @TargetPK = @SourcePk - MERGE SCRIPT
                        'is_null_merge_pk': [], # Used to check if the PK or composite key has null values
                        'delete_where_not_exists_type_casted_pk': [], # Used to typecast the pks as they have null values IFNULL(SAFE_CAST(s.col AS string),'') = IFNULL(SAFE_CAST(t.col AS string),'')
                    }

                # Add general schema components to schema_dict
                # schema_dict[table_name]['create_table_body'].append(f"{column_name} {bigquery_datatype},")
                schema_dict[inc_table_name]['schema_validator'].append(f"{column_name} {bigquery_datatype}")
                schema_dict[inc_table_name]['merge_insert'].append(f"{column_name},")
                schema_dict[inc_table_name]['merge_insert_source'].append(f"s.{column_name},")

                # Add primary key based components to schema_dict
                if pk == "PK":
                    # schema_dict[table_name]['create_pk_table_body'].append(f"{column_name} {bigquery_datatype},")
                    schema_dict[inc_table_name]['merge_pk'].append(f"{column_name},")
                    schema_dict[inc_table_name]['delete_where_not_exists'].append(f"t.{column_name} = s.{column_name} AND")
                    schema_dict[inc_table_name]['is_null_merge_pk'].append(f"pk.{column_name} IS NULL OR")
                    schema_dict[inc_table_name]['delete_where_not_exists_type_casted_pk'].append(f"IFNULL(SAFE_CAST(t.{column_name} AS STRING), '')  = IFNULL(SAFE_CAST(s.{column_name} AS STRING), '') AND")

                # Add merge components to schema_dict that omit the primary key
                else:
                    schema_dict[inc_table_name]['merge_when_matched'].append(f"t.{column_name} <> s.{column_name} OR")
                    schema_dict[inc_table_name]['merge_update_set'].append(f"t.{column_name} = s.{column_name},")

            return schema_dict
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIM4: PVO: {element.get('FILE_NAME')} Problem parsing the schema file", {e})
    
    def query_table_schema(element, bq_project, dataset, table_name):
        try:
            # Connecint to base table
            client = bigquery.Client()
            
            # Information schema query fo the base table
            table_stats_query = f"SELECT column_name, data_type FROM `{bq_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
            
            # Executing query and returning the schema
            query_job = client.query(table_stats_query)
            existing_table_schema = query_job.result()
            
            return list(existing_table_schema)
        
        except Exception as e:
            logging.fatal(f"PIPELINE ERROR CODE HBIM5: Problem with BQ schema query against `{bq_project}.{dataset}.{table_name}` Error:", {e})
            
    def compare_schema_files(self, pvo_name, existing_table_schema, element_schema_dict):
        
        # +2 as the base tables have created_at and updated_at
        if (len(existing_table_schema)) != (len(element_schema_dict) + 2): 
            logging.fatal(f"PIPELINE FATAL CODE HBIM6: PVO: {pvo_name} Existing Table Schema: {existing_table_schema} \n\nElement Schema Dict {element_schema_dict}")
            return False, []
        else:
            status_bit = 1
            
            # Creating a simple dictionary of the columns:datatypes for the basetable queried above
            existing_table_schema_simple_dict = {row[0]: row[1] for row in existing_table_schema}
            del existing_table_schema_simple_dict['updated_at'] # Removing updated_at
            del existing_table_schema_simple_dict['created_at'] # Removing created_at
            
            # Creating a simple dictionary of the columns:datatypes for the schema file
            element_schema_simple_dict = {row.split(" ")[0]: row.split(" ")[1] for row in element_schema_dict}
            
            # Create uppercase list of the column names from the schema file
            element_scheme_list = [key.upper() for key in element_schema_simple_dict.keys()] 
            
            # Dictionary Comparison - checks key-values are the same irrespective of ordinal position.     
            if existing_table_schema_simple_dict == element_schema_simple_dict:
                status_bit = status_bit
            else:
                status_bit = 0
            
            if status_bit == 1:
                return True, element_scheme_list
            else: 
                logging.fatal(f"SCHEMA FATAL CODE HBIM7: Schema column names or data type do not match.\n Existing Table Schema: {existing_table_schema_simple_dict} \n\nElement Schema Dict {element_schema_simple_dict}")
                return False, []

class CopyFilesToExternal(beam.DoFn):
    '''
    Used to copy the .csv and .pecsv files to the directory for the external tables.
    Does not further hydrate element.
    Args:   Element: Generator
            Str: unzipped_bucket
            Str: unzipped_prefix
            Str: bicc_external_prefix
                
    Yields:    Element: Generator
    '''
    def process(self, element, unzipped_bucket, unzipped_prefix, bicc_external_prefix):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 5 Starting: CopyFilesToExternal *********")

        # Connecting to unzipped bucket
        storage_client = storage.Client()
        storage_bucket = storage_client.bucket(unzipped_bucket)

        # Listing files 
        # -batch added to signify the end of batch, some PVOs have similar file names such as HCM_POSITION_POSITION and HCM_POSITION_POSITION_ALL
        source_files = storage_bucket.list_blobs(prefix=f"{unzipped_prefix}/{element.get('FILE_NAME')}-batch")
        
        # Generating directory names
        extract_directory_name = f"{element.get('EXTRACT').lower()}-external"
        table_directory_name = element.get('BASETABLE').upper()

        extract_external_prefix = f"{bicc_external_prefix}/{extract_directory_name}/{table_directory_name}"
        self.copy_with_retries(element, source_files, storage_bucket, extract_external_prefix, retry_count=0)

        
        data_elements = {
            'FILE_NAME': element.get('FILE_NAME'), #PVO
            'FILE_PATH': element.get('FILE_PATH'),
            'EXTRACT': element.get('EXTRACT'),
            'BASETABLE': element.get('BASETABLE'),
            'PKTABLE': element.get('PKTABLE'),
            'INCTABLE': element.get('INCTABLE'),
            'ELEMENT_SCHEMA_DICT': element.get('ELEMENT_SCHEMA_DICT'),
            'ELEMENT_SCHEMA_LIST': element.get('ELEMENT_SCHEMA_LIST'),
            'EXT_DATA_TABLE_SCHEMA': element.get('EXT_DATA_TABLE_SCHEMA'),
            'EXT_PK_TABLE_SCHEMA': element.get('EXT_PK_TABLE_SCHEMA'),
            'EXTERNAL_PATH': extract_external_prefix
        }
        logging.info(f"PIPELINE INFO: Successfully copied the data and pk files to the external folder for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')}")
        
        yield data_elements            
        
    def copy_with_retries(self, element, source_files, storage_bucket, extract_external_prefix, retry_count):
        
        # Copying the files from the source to the new external directories
        for file in source_files:
            try:
                if file.name.endswith(".csv") or file.name.endswith(".pecsv"):
                    source_blob = storage_bucket.blob(file.name)
                    destination_blob = storage_bucket.blob(f"{extract_external_prefix}/{file.name.split('/')[-1]}") 
                    destination_blob.rewrite(source_blob) 
                    logging.info(f"PIPELINE INFO: Attempt: - {retry_count} Copying: ({source_blob.name}) to the external folder ({destination_blob.name}) for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')}")
                
            except Exception as e:
                if retry_count < 4:
                    retry_count +=1
                    self.copy_with_retries(element, source_files, storage_bucket, extract_external_prefix, retry_count)
                else:
                    logging.error(f"PIPELINE ERROR CODE HBIM8: Problem copying the data and pk files (gs://{source_blob.name}) to the external folder ({destination_blob}) for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')}\nError:", {e})

class ListExternalTables(beam.DoFn):
    '''
    Lists the CSVs files in the external tables.
    It will fanout elements in the pipeline.
    '''
    def process(self, element, unzipped_bucket):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 6 Starting: List External Tables Files *********")
            
        storage_client = storage.Client()
        storage_bucket = storage_client.bucket(unzipped_bucket)

        source_files_csv = storage_bucket.list_blobs(prefix=f"{element.get('EXTERNAL_PATH')}/{element.get('FILE_NAME')}")
        source_files_csv = deque(source_files_csv)
        
        blob_count = sum(1 for _ in source_files_csv)
        
        for file in source_files_csv:
            data_elements = {
                'EXTERNAL_FILE': file.name,
                'FILE_NAME': element.get('FILE_NAME'), #PVO
                'FILE_PATH': element.get('FILE_PATH'),
                'EXTRACT': element.get('EXTRACT'),
                'BASETABLE': element.get('BASETABLE'),
                'PKTABLE': element.get('PKTABLE'),
                'INCTABLE': element.get('INCTABLE'),
                'ELEMENT_SCHEMA_DICT': element.get('ELEMENT_SCHEMA_DICT'),
                'ELEMENT_SCHEMA_LIST': element.get('ELEMENT_SCHEMA_LIST'),
                'EXT_DATA_TABLE_SCHEMA': element.get('EXT_DATA_TABLE_SCHEMA'),
                'EXT_PK_TABLE_SCHEMA': element.get('EXT_PK_TABLE_SCHEMA'),
                'EXTERNAL_PATH': element.get('ELEMENT_SCHEMA_LIST'),
                'TOT_EXTERNAL_FILE': blob_count,
                'TOT_EXTERNAL_FILE_REFERENCE': blob_count
                }
            
            yield data_elements

class CheckExternalTable(beam.DoFn):
    '''
    Verifies the interity of the csv files in the external table prior to processing.
    It will read the first line of the CSV file to verify the ordinal positions are the same as the mdcsv file (equal to the base table via transitive property)
    If the ordinal position is NOT the same, it will check to make sure all columns in the mdcsv are in the csv and visa versa. 
    If they ARE present but in the wrong order, it will correct the order.
    
    It does this as the external tables are sensitive to the ordinal position. 
    '''
    def process(self, element, unzipped_bucket):
        
        storage_client = storage.Client()
        storage_bucket = storage_client.bucket(unzipped_bucket)
        
        source_external_table_file = storage_bucket.get_blob(element.get('EXTERNAL_FILE'))

        ext_data_table_schema_list = element.get('EXT_DATA_TABLE_SCHEMA')
        ext_pk_table_schema_list = element.get('EXT_PK_TABLE_SCHEMA')
    
        if source_external_table_file.name.endswith(".csv"):
            source_blob = storage_bucket.blob(source_external_table_file.name)
            
            # .CSV files related to the External Data Table
            self.check_file(unzipped_bucket,storage_bucket,source_blob,ext_data_table_schema_list,element.get('INCTABLE'))
        
        elif source_external_table_file.name.endswith(".pecsv"):
            source_blob = storage_bucket.blob(source_external_table_file.name)
            
            # .CSV files related to the External Data Table
            self.check_file(unzipped_bucket,storage_bucket,source_blob,ext_pk_table_schema_list,element.get('PKTABLE'))
        
        
        data_elements = {
            'FILE_NAME': element.get('FILE_NAME'), #PVO
            'FILE_PATH': element.get('FILE_PATH'),
            'EXTRACT': element.get('EXTRACT'),
            'BASETABLE': element.get('BASETABLE'),
            'PKTABLE': element.get('PKTABLE'),
            'INCTABLE': element.get('INCTABLE'),
            'ELEMENT_SCHEMA_DICT': element.get('ELEMENT_SCHEMA_DICT'),
            'ELEMENT_SCHEMA_LIST': element.get('ELEMENT_SCHEMA_LIST'),
            'EXTERNAL_PATH': element.get('ELEMENT_SCHEMA_LIST'),
            'TOT_EXTERNAL_FILE': element.get('TOT_EXTERNAL_FILE'),
            'TOT_EXTERNAL_FILE_REFERENCE': element.get('TOT_EXTERNAL_FILE_REFERENCE')
            }
        
        yield data_elements
    
    def cols_in_list1_not_in_list2(self, list1, list2):
        '''
        This will return the columns as a list in list1 not in list2, 
        comparing case-insensitively by converting everything to UPPERCASE.
        '''
        list2_upper = [column_name.upper() for column_name in list2]
        return [column_name for column_name in list1 if column_name.upper() not in list2_upper]
            
    def check_file(self, unzipped_bucket, storage_bucket, source_blob, existing_schema_list, external_table_name):
        
        first_line = ""                
        with source_blob.open("r") as file:
            
            # Read and capture the first line
            csv_file_header = file.readline().replace("\n", "").split(',')
                                
            # Checking if the all the headers in the CSV or PECSV files are the same position.
            if len(existing_schema_list) != len(csv_file_header):
                columns_in_ext_tbl_not_in_csv_file_header = self.cols_in_list1_not_in_list2(existing_schema_list, csv_file_header)
                if len(columns_in_ext_tbl_not_in_csv_file_header) > 0 : logging.error(f"PIPELINE ERROR: # of col [{len(existing_schema_list)}] in external table: {external_table_name} is GREATER than # [{len(csv_file_header)}] of file - gs://{unzipped_bucket}/{source_blob.name} \n extra cols in external table: {columns_in_ext_tbl_not_in_csv_file_header}")
                
                columns_in_csv_file_header_not_in_ext_tbl = self.cols_in_list1_not_in_list2(csv_file_header,existing_schema_list)
                if len(columns_in_csv_file_header_not_in_ext_tbl) > 0 : logging.error(f"PIPELINE ERROR: # of col [{len(existing_schema_list)}] in external table: {external_table_name} is LESS than # [{len(csv_file_header)}] of file - gs://{unzipped_bucket}/{source_blob.name} \n extra cols in file: {columns_in_csv_file_header_not_in_ext_tbl}")
            else:
                
                # Checking the ordinal position is the same
                all_ordinal_check = all(row1 == row2 for row1, row2 in zip(existing_schema_list, csv_file_header))
                
                if not all_ordinal_check:
                    blob_name = f"{source_blob.name}"
                    file_type = blob_name.split('.')[-1]


                    # Ordinal position of columne names is different
                    
                    # Checking if all the columns are present just in the wrong ordinal position. 
                    columns_in_mdcsv_not_in_csv_header = self.cols_in_list1_not_in_list2(existing_schema_list,csv_file_header)
                    # columns_in_mdcsv_not_in_csv_header = [column_name for column_name in existing_schema_list if column_name not in csv_file_header]
                    columns_in_csvheader_not_in_existing_table = self.cols_in_list1_not_in_list2(csv_file_header, existing_schema_list)
                    # columns_in_csvheader_not_in_existing_table = [column_name for column_name in csv_file_header if column_name not in existing_schema_list]
                
                    if len(columns_in_mdcsv_not_in_csv_header) != 0:
                        raise Exception(f"PIPELINE ERROR CODE HBIM9: The following columns are in the existing ext table ({external_table_name}) and not in the {file_type} file - {columns_in_mdcsv_not_in_csv_header} File - gs://{unzipped_bucket}/{source_blob.name}")
                    if len(columns_in_csvheader_not_in_existing_table) != 0:
                        raise Exception(f"PIPELINE ERROR CODE HBIM10: The following columns are in the {file_type} files and not in the existing table ({external_table_name}) -  {columns_in_csvheader_not_in_existing_table} File - gs://{unzipped_bucket}/{source_blob.name}")
                    else:
                        logging.warning(f"PIPELINE WARNING: Ordinal position of {file_type} file gs://{unzipped_bucket}/{source_blob.name} and external table ({external_table_name}) are different")
                        
                        # Correcting the order of the rows
                        csv_content = source_blob.download_as_text()

                        # Capture header line and split by comma
                        csv_file_header = csv_content.splitlines()[0].split(',')  
                        
                        # Open the CSV reader and read the rows
                        reader = csv.DictReader(
                            csv_content.splitlines(),
                            quoting=csv.QUOTE_ALL,
                            quotechar='"'
                            ) #, fieldnames=csv_file_header

                        # Get the fieldnames (columns) from the csv/pecsv
                        fieldnames = reader.fieldnames

                        # Reorder columns according to the desired order
                        reordered_fieldnames = [name for name in existing_schema_list if name in fieldnames]

                        # Prepare the output CSV data
                        output_csv = StringIO()
                        writer = csv.DictWriter(
                            output_csv,
                            fieldnames=reordered_fieldnames,
                            quoting=csv.QUOTE_ALL,
                            quotechar='"',
                            escapechar='\\',
                            lineterminator='\n'
                            )

                        # Write the header (columns) to the output csv/pecsv
                        writer.writeheader()

                        # Write the rows to the output CSV with the columns reordered
                        for row in reader:
                            reordered_row = {col: row[col] for col in reordered_fieldnames}
                            writer.writerow(reordered_row)

                        # Upload the processed CSV data to GCS
                        output_blob = storage_bucket.blob(source_blob.name)
                        output_blob.upload_from_string(output_csv.getvalue(), content_type='text/csv')

                        logging.warning(f"PIPELINE WARNING: Corrected ordinal position of {file_type}: {source_blob.name}. Uploaded gs://{unzipped_bucket}/{source_blob.name}")                            
                else:
                    logging.debug(f"PIPELINE DEBUG: Quality of {source_blob.name} File OK gs://{unzipped_bucket}/{source_blob.name}")
                                                                

class CombineElements(beam.CombineFn):
    '''
    Apply CombineGlobally on each group. This combines all the grouped elements. 
    There is only 1 group of N elements. With N being the number of CSV files in the external-tables location.
    The TOT_EXTERNAL_FILE value will equal N*N
    It is grouped by PVO in the previous Beam Map stage.
    '''
    def create_accumulator(self):
        # Initialize an accumulator to store the combined result
        return {
            'FILE_NAME': None,
            'FILE_PATH': None,
            'EXTRACT': None,
            'BASETABLE': None,
            'PKTABLE': None,
            'INCTABLE': None,
            'ELEMENT_SCHEMA_DICT': None,
            'ELEMENT_SCHEMA_LIST': None,
            'EXTERNAL_PATH': None,
            'TOT_EXTERNAL_FILE': 0,  # Initially 0, will accumulate the count (N*N)
            'TOT_EXTERNAL_FILE_REFERENCE': 0 # Intially 0, will remain at the intial count (N)
            # These two can be used to verify all elements are present on the fanout return by sqrt(N*N) = N
        }
    
    def add_input(self, accumulator, element):
        # Since the elements are the same, just pick one
        if accumulator['TOT_EXTERNAL_FILE'] == 0:
            # Set the first element's values
            accumulator['FILE_NAME'] = element.get('FILE_NAME')
            accumulator['FILE_PATH'] = element.get('FILE_PATH')
            accumulator['EXTRACT'] = element.get('EXTRACT')
            accumulator['BASETABLE'] = element.get('BASETABLE')
            accumulator['PKTABLE'] = element.get('PKTABLE')
            accumulator['INCTABLE'] = element.get('INCTABLE')
            accumulator['ELEMENT_SCHEMA_DICT'] = element.get('ELEMENT_SCHEMA_DICT')
            accumulator['ELEMENT_SCHEMA_LIST'] = element.get('ELEMENT_SCHEMA_LIST')
            accumulator['EXTERNAL_PATH'] = element.get('EXTERNAL_PATH')
            accumulator['TOT_EXTERNAL_FILE_REFERENCE'] = element.get('TOT_EXTERNAL_FILE_REFERENCE')
        
        # Accumulate the TOT_EXTERNAL_FILE count
        accumulator['TOT_EXTERNAL_FILE'] += element.get('TOT_EXTERNAL_FILE', 0)
        
        return accumulator

    def merge_accumulators(self, accumulators):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 8 Starting: Combine Elements *********")
        
        # Merge multiple accumulators
        result = self.create_accumulator()
        for accumulator in accumulators:
            # Merge fields (we assume that they are identical so we can just take the first one)
            result['TOT_EXTERNAL_FILE'] += accumulator['TOT_EXTERNAL_FILE']
            result['FILE_NAME'] = accumulator['FILE_NAME']  # All elements are the same
            result['FILE_PATH'] = accumulator['FILE_PATH']
            result['EXTRACT'] = accumulator['EXTRACT']
            result['BASETABLE'] = accumulator['BASETABLE']
            result['PKTABLE'] = accumulator['PKTABLE']
            result['INCTABLE'] = accumulator['INCTABLE']
            result['ELEMENT_SCHEMA_DICT'] = accumulator['ELEMENT_SCHEMA_DICT']
            result['ELEMENT_SCHEMA_LIST'] = accumulator['ELEMENT_SCHEMA_LIST']
            result['EXTERNAL_PATH'] = accumulator['EXTERNAL_PATH']
            result['TOT_EXTERNAL_FILE_REFERENCE'] = accumulator['TOT_EXTERNAL_FILE_REFERENCE']
            
        return result

    def extract_output(self, accumulator):
        # Return the combined result
        return accumulator

class CheckingFanoutReturnedSuccessfully(beam.DoFn):
    '''
    This is a simple class to verify that SQRT(N**N)=N
    Meaning the all the elements that were sent in the fanout executed and returned to the group sent to the accumulator. 
    It is important that all elements returned before executing the Merge SQL query exactly once.
    '''
    def process(self, element):
        # Obtaining N - value stored in the element 
        number_of_elements = element[1].get('TOT_EXTERNAL_FILE_REFERENCE')
        # Obtaining N*N - value store in the group of elements
        number_of_elements_aggregated = element[1].get('TOT_EXTERNAL_FILE')    
        
        # Checking SQRT(N*N) = N
        if int(number_of_elements)==int(number_of_elements_aggregated**0.5):
            
            # Returning only the external files
            yield element[1]
            
        elif int(number_of_elements) < int(number_of_elements_aggregated**0.5):
            raise ValueError("PIPELINE FATAL CODE HBIM11: There may duplicate csv files in bicc-external folder. Clear bicc-zipped files and external tables and restart the DAG.")
        
        else:
            raise ValueError("PIPELINE FATAL CODE HBIM11: Elements were lost during the external CSV file fanout - rerun the pipeline.")

class MergeScript(beam.DoFn):
    '''
    Used to generate and execute the MergeScript
    Args:   Element: Fully Hydrated Generator
            Str: bq_project
            Str: dump_dataset
            Str: base_dataset
                
    Returns:
    '''
    def process(self, element, bq_project, dump_dataset, base_dataset, csv_map_bucket, start_time):
        logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 9 Starting: Generate and Execute Dynamic SQL *********")

        # Obtaining information from the elements
        schema_dict, base_table, inc_table, pk_table = element.get("ELEMENT_SCHEMA_DICT"), element.get("BASETABLE"), element.get("INCTABLE"), element.get("PKTABLE")

        # Creating SQL query
        sql_query = self.create_merge_script(element, schema_dict, base_table, inc_table, pk_table, bq_project, dump_dataset, base_dataset)
        
        # Executing SQL
        sql_status,sql_message  = self.execute_sql(element, bq_project, sql_query)
        
        # Logic to categorize and deadletter failed SQL merge scripts
        if sql_status == 1:
            successful_query_destination_blob_name = f"merge-dynamic-sql/{element.get('EXTRACT')}/latest-successful-{element.get('FILE_NAME')}.sql"
            self.upload_query_to_gcs(element, csv_map_bucket, successful_query_destination_blob_name, sql_query)
            logging.info(f"PIPELINE INFO: SUCCESSFULLY merge for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')} - {sql_message}")
        elif sql_status == 2:
            failed_query_destination_blob_name = f"merge-dynamic-sql/{element.get('EXTRACT')}/deadletter/{start_time.split('T')[0]}-{element.get('FILE_NAME')}.sql"
            self.upload_query_to_gcs(element, csv_map_bucket, failed_query_destination_blob_name, sql_query)
            logging.error(f"PIPELINE ERROR HBIM12: BQ executed but FAILED merge for Table: {element.get('BASETABLE')} in Extract: {element.get('EXTRACT')} - {sql_message}")
        elif sql_status == 3:
            failed_query_destination_blob_name = f"merge-dynamic-sql/{element.get('EXTRACT')}/deadletter/{start_time.split('T')[0]}-{element.get('FILE_NAME')}.sql"
            self.upload_query_to_gcs(element, csv_map_bucket, failed_query_destination_blob_name, sql_query)
            logging.error(f"PVO:{element.get('FILE_NAME')} - {sql_message}")


        yield element
    
    def upload_query_to_gcs(self, element, bucket_name, destination_blob_name, query_string):
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
                
        # Create a new blob object for the file
        blob = bucket.blob(destination_blob_name)

        # Upload the string directly to GCS
        blob.upload_from_string(query_string, content_type="text/plain") 

    def create_merge_script(self, element, schema_dict, base_table, inc_table, pk_table, bq_project, dump_dataset, base_dataset):
            merge_script = ""
            for table_name, components in schema_dict.items():
                
                # Setting components to avoid using \n within an f string
                component_merge_pk = " \n".join(components['merge_pk']).rstrip(',')
                component_del_where_not_exist = " \n".join(components['delete_where_not_exists']).rstrip(' AND')
                is_null_check_component_merge_pk = " \n".join(components['is_null_merge_pk']).rstrip(' OR')
                type_casted_component_del_where_not_exist = " \n".join(components['delete_where_not_exists_type_casted_pk']).rstrip(' AND')
                component_merge_when_matched = " \n".join(components['merge_when_matched']).rstrip('OR')
                
                # Big Query cannot cluster a temp table by greater than 4 columns
                cluster_var = f"CLUSTER BY {component_merge_pk}"
                
                if len(component_merge_pk) > 4: 
                        cluster_var = ''
        
                merge_script += f"""
                DECLARE return_message_dedup STRING;
                DECLARE return_message_merge STRING;
                DECLARE return_message_pk_del STRING;
                DECLARE return_error_message_trans_1 STRING;
                DECLARE return_error_message_trans_2 STRING;
                DECLARE return_error_message_trans_3 STRING;
                DECLARE count_ext_data_table BOOL;
                DECLARE count_ext_pk_table BOOL;
                DECLARE illegal_null_pks_not_exist BOOL;
                
                -- Transaction #1 - Create and Dedupe Source Table To Use In Merge
                BEGIN
                                
                    BEGIN TRANSACTION;
                        SET return_error_message_trans_1 = ''; 
                        SET count_ext_data_table = EXISTS (SELECT 1 FROM `{bq_project}.{dump_dataset}.{inc_table}`);
                        SET illegal_null_pks_not_exist = NOT EXISTS(
                            SELECT 
                            1 
                            FROM `{bq_project}.{dump_dataset}.{pk_table}` pk
                            WHERE
                                {is_null_check_component_merge_pk}
                            );
                            
                        IF count_ext_data_table THEN
                            -- Dedupe Source Table To Use In Merge
                            CREATE OR REPLACE TEMP TABLE table_to_merge
                                -- Clustering: reduce scans by grouping related rows 
                            {cluster_var}
                            AS (
                                SELECT 
                                    *
                                FROM `{bq_project}.{dump_dataset}.{inc_table}` 
                                QUALIFY ROW_NUMBER() OVER (PARTITION BY {component_merge_pk}) = 1);
                            SET return_message_dedup = 'OK';
                            
                        ELSE
                            SET return_message_dedup = 'SKIP';
                        
                        END IF;
                        
                        COMMIT TRANSACTION;
                
                -- Rollback and Clean-Up Temp Table If There Is An Error
                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    SET return_error_message_trans_1 = CONCAT('Transaction 1 Dedup Temp Table Creation Failed at', CURRENT_TIMESTAMP(),' Error: ',@@error.message);
                    ROLLBACK TRANSACTION;
                    IF count_ext_data_table THEN
                        DROP TABLE IF EXISTS table_to_merge;
                    END IF;
                    
                END;
                
                -- Transaction #2 - Merge Source to Target
                BEGIN
                
                    BEGIN TRANSACTION; 
                    SET return_error_message_trans_2 = '';
                    
                    IF count_ext_data_table AND illegal_null_pks_not_exist THEN
                        -- Update rows in target 't' that have changed in source 's'.  Insert rows from source 's' that are not present in 't'.
                        MERGE `{bq_project}.{base_dataset}.{base_table}` t
                        USING table_to_merge s
                            ON {component_del_where_not_exist}
                        WHEN MATCHED AND ( -- When PK matches and columns differ, update target to match source.
                        {component_merge_when_matched}
                        ) THEN
                                UPDATE
                                SET {" ".join(components['merge_update_set'])}
                                    t.updated_at = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED
                            THEN
                                INSERT (
                                    {" ".join(components['merge_insert'])}
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    {" ".join(components['merge_insert_source'])}
                                    CURRENT_TIMESTAMP(),
                                    CAST(NULL AS TIMESTAMP)
                                    );
                    
                        -- cleanup: Drop temp table once its job is done
                        DROP TABLE table_to_merge;
                        
                        SET return_message_merge = 'OK';
    
                    END IF;
                    
                    IF count_ext_data_table AND NOT illegal_null_pks_not_exist THEN
                        -- Update rows in target 't' that have changed in source 's'.  Insert rows from source 's' that are not present in 't'. PKs have Nulls, use typecased PK. 
                        MERGE `{bq_project}.{base_dataset}.{base_table}` t
                        USING table_to_merge s
                            ON {type_casted_component_del_where_not_exist}
                        WHEN MATCHED AND ( -- When PK matches and columns differ, update target to match source.
                        {component_merge_when_matched}
                        ) THEN
                                UPDATE
                                SET {" ".join(components['merge_update_set'])}
                                    t.updated_at = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED
                            THEN
                                INSERT (
                                    {" ".join(components['merge_insert'])}
                                    created_at,
                                    updated_at
                                    )
                                VALUES (
                                    {" ".join(components['merge_insert_source'])}
                                    CURRENT_TIMESTAMP(),
                                    CAST(NULL AS TIMESTAMP)
                                    );
                    
                        -- cleanup: Drop temp table once its job is done
                        DROP TABLE table_to_merge;
        
                        SET return_message_merge = 'PK HAS NULLs';
                    
                    ELSE 
                        SET return_message_merge = 'SKIP';  
                                                
                    END IF;
                    
                    COMMIT TRANSACTION;
                    
                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    SET return_error_message_trans_2 = CONCAT('Transaction 2 Merge Failed at', CURRENT_TIMESTAMP(),' Error: ',@@error.message);
                    ROLLBACK TRANSACTION;
                
                END;
                
                -- Transaction #3 - Executing PK clean up
                BEGIN
                
                    BEGIN TRANSACTION;
                    
                    SET return_error_message_trans_3 = '';
                    SET count_ext_pk_table = EXISTS (SELECT 1 FROM `{bq_project}.{dump_dataset}.{pk_table}`);
                    
                    IF count_ext_pk_table AND illegal_null_pks_not_exist THEN 
                        -- Remove rows in the base table where the pk is not present in the pk table
                        DELETE FROM `{bq_project}.{base_dataset}.{base_table}` t
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM `{bq_project}.{dump_dataset}.{pk_table}` s
                            WHERE {component_del_where_not_exist}
                            );
                        
                        SET return_message_pk_del = 'OK';
                    END IF;
                    
                    IF count_ext_pk_table AND NOT illegal_null_pks_not_exist THEN
                        -- Remove rows in the base table where the pk is not present in the pk table
                        DELETE FROM `{bq_project}.{base_dataset}.{base_table}` t
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM `{bq_project}.{dump_dataset}.{pk_table}` s
                            WHERE {type_casted_component_del_where_not_exist}
                            );
                            
                        SET return_message_pk_del = 'PK HAS NULLs';
                        
                    ELSE
                        SET return_message_pk_del = 'SKIP';
                        
                    END IF;
                    COMMIT TRANSACTION;
                    
                -- Rollback merge and delete if something fails, return error message
                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    SET return_error_message_trans_3 = CONCAT('Transaction 3 Base Table PK Cleanup Failed at', CURRENT_TIMESTAMP(),' Error: ',@@error.message);
                    ROLLBACK TRANSACTION;
                
                END;
                
                SELECT 
                    CASE
                        WHEN return_error_message_trans_1 = '' AND return_error_message_trans_2 = '' AND return_error_message_trans_3 = ''
                            THEN 1
                        ELSE 0
                    END AS ok_status_bit,
                    CASE
                        WHEN return_message_dedup = 'OK' AND return_message_merge = 'OK' AND return_message_pk_del = 'OK' 
                            THEN 'Merge and Base Table PK Cleanup OK'
                        WHEN return_message_dedup = 'OK' AND return_message_merge = 'PK HAS NULLs' AND return_message_pk_del = 'PK HAS NULLs' 
                            THEN 'Merge and Base Table PK Cleanup OK - PK HAS NULLs'                              --OK
                        WHEN (return_message_dedup = 'SKIP' OR return_message_merge = 'SKIP') AND return_message_pk_del = 'OK' 
                            THEN 'Merge Skipped (Empty Ext Data Tbl) and Base Table PK Cleanup OK'                                --OK
                        WHEN return_message_dedup = 'OK' AND return_message_merge = 'OK' AND return_message_pk_del = 'SKIP' 
                            THEN 'Merge OK and Base Table PK Cleanup Skipped (Empty Ext PK Tbl)'
                        WHEN return_message_dedup = 'SKIP' AND return_message_merge = 'SKIP' AND return_message_pk_del = 'SKIP' 
                            THEN 'Merge Skipped (Empty Ext Data Tbl) and Base Table PK Cleanup Skipped (Empty Ext PK Tbl)'        --OK
                        WHEN return_error_message_trans_1 <> '' 
                            THEN return_error_message_trans_1
                        WHEN return_error_message_trans_2 <> '' 
                            THEN return_error_message_trans_2
                        ELSE return_error_message_trans_3
                    END AS return_message;
                """
                return merge_script
    
    def execute_sql(self, element, bq_project, query):
        try:
            client = bigquery.Client(project=bq_project)
            query_job = client.query(query)
            result = query_job.result()
            return_message_dict = dict(next(result))
            
            result_message = return_message_dict.get('return_message')
            print(f"TESTING RESULT MESSAGE{result_message}")
            if str(return_message_dict.get('ok_status_bit')) == '1':
                return 1, result_message
            else:
                return 2, result_message
            
        except Exception as e:
            return 3, f"PIPELINE ERROR HBIM13: Problem executing BQ Merge script \n************\nError:{e}"

def run_pipeline(unzipped_bucket, unzipped_prefix, csv_map_bucket, csv_prefix, csv_map_file, bq_project, dump_dataset, base_dataset, bicc_external_prefix):
    logging.info(f"PIPELINE INFO: ##################### Initiating Merge Pipeline  ##################### \nwith parameters:\n--unzipped_bucket={unzipped_bucket}\n--unzipped_prefix={unzipped_prefix}\n--csv_map_bucket={csv_map_bucket}\n--csv_prefix={csv_prefix}\n--csv_map_file={csv_map_file}\n--bq_project={bq_project}\n--dump_dataset={dump_dataset}\n--base_dataset={base_dataset}\n--bicc_external_prefix={bicc_external_prefix}\n")

    options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True

    # Creating runtime
    utc_now = datetime.now(timezone.utc)
    start_time = utc_now.strftime('%Y-%m-%dT%H:%M:%S') + f".{utc_now.microsecond:06d}"
    logging.info(f"PIPELINE INFO: start time: {start_time}")
    logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 1 Starting: List Files *********")
    with beam.Pipeline(options=options) as p:
        (p | "List Files" >> fileio.MatchFiles(f'gs://{unzipped_bucket}/{unzipped_prefix}/*.mdcsv', empty_match_treatment=fileio.EmptyMatchTreatment.DISALLOW)
           | 'Reshuffle 1' >> beam.Reshuffle()
           | 'Logging Ingested Files' >> beam.ParDo(IngestSchemaFiles())
           | 'Reading the MapperCSV' >> beam.ParDo(CheckMapperCSV(), csv_map_bucket, csv_prefix, csv_map_file)
           | 'Parse Schema File' >> beam.ParDo(ParseSchema(), unzipped_bucket, bq_project, base_dataset, dump_dataset)
           | 'Copy .CSV and .PECSV Files to BICC External' >> beam.ParDo(CopyFilesToExternal(), unzipped_bucket, unzipped_prefix, bicc_external_prefix)
           | 'List External Files and Fanout' >> beam.ParDo(ListExternalTables(), unzipped_bucket) 
           | 'Reshuffle 2' >> beam.Reshuffle()
           | 'Verify Integrity of External Table in Fanout' >> beam.ParDo(CheckExternalTable(), unzipped_bucket)
           | "Group by TOT_EXTERNAL_FILE combine in Group" >> beam.Map(lambda x: (x['BASETABLE'], x))  # Key by Basetable AKA Pvo
           | "Combine by Key merge fannedout elements in Group" >> beam.CombinePerKey(CombineElements())  
           | "Checking the fanout returned successfully" >> beam.ParDo(CheckingFanoutReturnedSuccessfully())
           | 'Merge Script SQL Execution' >> beam.ParDo(MergeScript(), bq_project, dump_dataset, base_dataset, csv_map_bucket, start_time)
        )
    
if __name__ == '__main__':

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    # logging.getLogger().setLevel(logging.INFO)

    logging.info("PIPELINE INFO: Hello WELCOME to Data Reconcilation!!")

    try:

        parser = argparse.ArgumentParser(description='Data Merge DataFlow program')
        parser.add_argument('--unzipped_bucket', type=str, required=True, help='')
        parser.add_argument('--unzipped_prefix', type=str, required=True, help='')
        parser.add_argument('--csv_map_bucket', type=str, required=True, help='')
        parser.add_argument('--csv_prefix', type=str, required=True, help='')        
        parser.add_argument('--csv_map_file', type=str, required=True, help='')
        parser.add_argument('--bq_project', type=str, required=True, help='')
        parser.add_argument('--dump_dataset', type=str, required=True, help='')
        parser.add_argument('--base_dataset', type=str, required=True, help='')
        parser.add_argument('--bicc_external_prefix', type=str, required=True, help='')

        args, pipeline_args = parser.parse_known_args()

        # Checking the pipeline arguments
        if "/" in args.unzipped_bucket:
            raise argparse.ArgumentTypeError(f"--unzipped_bucket: {args.unzipped_bucket} contains "/" or subpath must be top level storage bucket")
        if "/" in args.csv_map_bucket:
            raise argparse.ArgumentTypeError(f"--csv_map_bucket: {args.csv_map_bucket} contains "/" or subpath must be top level storage bucket")
        if args.unzipped_prefix.startswith("/") or args.unzipped_prefix.endswith("/"):
            raise argparse.ArgumentTypeError(f"--unzipped_prefix: {args.unzipped_prefix} starts or ends with "/". May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
        if args.csv_prefix.startswith("/") or args.unzipped_prefix.endswith("/"):
            raise argparse.ArgumentTypeError(f"--csv_prefix: {args.csv_prefix} starts or ends with "/". May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
        if args.bicc_external_prefix.startswith("/") or args.unzipped_prefix.endswith("/"):
            raise argparse.ArgumentTypeError(f"--bicc_external_prefix: {args.bicc_external_prefix} starts or ends with "/". May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")


        run_pipeline(args.unzipped_bucket, args.unzipped_prefix, args.csv_map_bucket, args.csv_prefix, args.csv_map_file, args.bq_project, args.dump_dataset, args.base_dataset, args.bicc_external_prefix)

    except fileio.filesystem.BeamIOError as e:
            logging.error(f"PIPELINE ERROR CODE HBIM1: PIPELINE STAGE 1 'List Files'. {e}")
            
    except Exception as e:
            raise TypeError(f"PIPELINE FATAL ERROR CODE HBIM99: in __main__ , Please Investigate :- {e}") 
