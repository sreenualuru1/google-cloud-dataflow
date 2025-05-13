"""
This script is to be used with the incremental harwick extracts. It verifies two things:
1. EXT_ Incremental Data Extract Status JSON Record Count vs Count of rows in the External Data Tables.
2. BS_ Primary Key Extract Status JSON Record Count vs Count of rows in the Base Tables. 

Notes on the required arguments. 
    1. Location of the JSON files
        gs://{zipped_bucket}/{json_prefix}/{extract}*.JSON'
    2. Location of Mapper CSV File
        gs://{flex_template_bucket}/{csv_prefix}/{csv_map_file}
    3. Location of Base Table (compared to Primary Key Status JSON)
        {bq_project}.{bq_basetable_source_dataset}.tablename
        tablename - derived from mapper file
    4. Location of Inc External Table (compared to Data Status JSON)
        {bq_project}.{bq_inc_external_dataset}.tablename
        tablename - derived from mapper file
    5. Destination Dataset
        {bq_project}.{bq_target_recon_dataset}.{bq_table_base_recon}
        Prefix Usage:
        Data Element - EXT_ = Inc External Tables == Data Status Json
        PK Element - BS_ = Base Tables = Primary Key Status Json
--------------------------MAJOR CHANGE LOG--------------------------
Version 1.0.0
Description: Original Harwick-Ofusion Incremental Archive Job
Date: 2024-11-13
Author: Zach Montoya
"""
import argparse
import apache_beam as beam
import csv
import json
import logging
import os
import time

from datetime import datetime, timezone
from apache_beam.io import fileio
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import DeadlineExceeded

class LogIngestedFiles(beam.DoFn):
    '''
    Ingesting elements and yielding as a generator, logging ingestion. 
    Args:   Element: FileMetadata Objects 
    Yields: Element: Generator
    '''
    def process(self, element):
        logging.info(f"PIPELINE INFO: ********* Starting Pipeline Stage 2: Logging Ingested Files  ********* ")
        logging.info(f"PIPELINE INFO: Ingested file: {element.path}")
        yield element

class ReadJSON(beam.DoFn):
    '''
    Reads the contents of the JSON file and yields 1 generator per JSON
    Args:   Element: File_metata in Generator
    Yields: Element: Hydrated Generator
    '''
    def process(self, file_metadata):
        logging.info(f"PIPELINE INFO: ********* Starting Pipeline Stage 3: ReadJSON  ********* ")

        try:
            with file_metadata.open() as f:
                
                json_content = f.read().decode('utf-8')
                json_data = json.loads(json_content)
                data = json_data["statuses"]

                for table in data:

                    # Extracting the filename from the apache_beam.io.fileio.ReadableFile object
                    absolute_filepath = file_metadata.metadata.path
                    filename = os.path.basename(absolute_filepath)
                    extract_list = filename.split('_')
        
                    if "KEYS" in extract_list:
                        extract_type = 'PRIMARY_KEYS'
                    elif 'DATA' in extract_list:
                        extract_type = 'DATA'
                    else:
                        raise Exception(f'Error - JSON file undefined as DATA or PRIMARY_KEYA {filename}')
                    
                    # Checking the extract type is correct
                    if extract_type == 'DATA' or extract_type == 'PRIMARY_KEYS':
                    
                        # Reading from the status file to obtain the PVO name and status counts
                        table_json = json.loads(json.dumps(table))
                        pvo_name = f'{table_json["name"].casefold()}'
                        status_count = int(table_json['rowCount'])

                        data_elements = {
                            'EXTRACT_TYPE': extract_type, 
                            'PVO_NAME': pvo_name, 
                            'RECORD_COUNT': status_count
                        }
                        
                        # Logging and yielding 1 generator per JSON read
                        logging.info(f"PIPELINE INFO: PVO-{pvo_name} Read status file: {filename}")

                        yield data_elements
                    else:
                        logging.error(f"PIPELINE ERROR CODE HBIR2: Stage 'Extract JSON Data' Problem indexing the JSON file {file_metadata}")
                    
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR3: Problem reading the JSON status files. ERROR :- ", {e})

class CheckMapperCSV(beam.DoFn):
    '''
    Reads a mapper CSV file that relates PVO name to base table names, incremental table names, and primary key table names. 
    Args:   Element: Generator
            Str: csv_map_bucket
            Str: csv_prefix
            Str: csv_map_file
    Yields: Element: Hydrated Generator
    '''
    def process(self, element, csv_map_bucket ,csv_prefix, csv_map_file):

        try:
            # logging.info(f"PIPELINE INFO: ********* Starting Pipeline Stage 5: CheckMapperCSV  ********* ")
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
                if element.get('PVO_NAME').lower() == row[2]:
                    not_found = False
                    
                    # If the PVO_NAME is found, add TABLENAME to the row
                    data_elements = {
                        'EXTRACT_TYPE': element.get('EXTRACT_TYPE'), 
                        'PVO_NAME': element.get('PVO_NAME'),
                        'BICC_EXTRACT': row[3],  
                        'RECORD_COUNT': element.get('RECORD_COUNT'), 
                        'TABLENAME': row[1],
                        'INC_TABLE': row[8],
                        'PK_TABLE': row[7],
                        'IEI_TABLE': row[9]
                    }
                    logging.info(f"PIPELINE INFO: Stage 5 - Successfully read the CSV Mapper File for {element.get('EXTRACT_TYPE')} of {element.get('PVO_NAME')}")
                    yield data_elements
                    break

            if not_found:
                # If the PVO_NAME is not found, add message to row
                data_elements = {
                    'EXTRACT_TYPE': element.get('EXTRACT_TYPE'), 
                    'PVO_NAME': element.get('PVO_NAME'), 
                    'BICC_EXTRACT': "PVO NOT IN MAPPER",  
                    'RECORD_COUNT': element.get('RECORD_COUNT'),
                    'TABLENAME': "PVO NOT IN MAPPER",
                    'INC_TABLE': "PVO NOT IN MAPPER",
                    'PK_TABLE': "PVO NOT IN MAPPER",
                    'IEI_TABLE': "PVO NOT IN MAPPER"
                }
                
                logging.warning(f"PIPELINE WARNING CODE HBIR4: ELEMENTS MISSING FROM CSV MAPPER FILE: {data_elements}")
                yield data_elements

            storage_client.close()

        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR5: Error reading the CSV Mapper file. ERROR :- ", {e})

class QueryBQForTable(beam.DoFn):
    '''
    Checks the row counts of the basetables and the incremental data extracts based on the table names from the mapper file
    Below is the relationship of the queries.
        1. The ELEMENT that is based on the Primary Key Status JSON File
            Checks the row count of the IEI table
                If the IEI table is empty do not delete records
                If the IEI table is NOT empty
                    Delete IEIs in IEI table that are NOW in the base table - resolved IEI
                    Delete IEIs in IEI table that are NOT in the PK table - resolved IEI
                Checks the row count of the IEI table after the deletes
            Stores the value of the IEI table after deletes for later validation - should be 0 if all is resolved
            Inserts NEW IEIs into the IEI table, these are PKs in the External PK table that are NOT in the base table
            Check the row count of the IEI table after inserts and store the value for later validation
            Queries the TABLE_NAME aka Base Table
            Compares to the Primary Key Status JSON record Count. 
        2. The ELEMENT that is based on the Data Status JSON File
            Queries the External Data Tble aka INC_<Base_Table_Name>
            Compares to the Data Status JSON record Count. 
    Args:   Element: Generator
            Str: bq_project
            Str: bq_basetable_source_dataset
            Str: bq_inc_external_dataset
            Str: run_time
    Yields: Element: Hydrated Generator
    '''
    def process(self, element, bq_project, bq_basetable_source_dataset, bq_inc_external_dataset, bq_iei_dataset, run_time):
        logging.info(f"PIPELINE INFO: Stage 7 - Starting QueryBQForTable for {element.get('PVO_NAME')} ")

        ext_data_table_name = element.get("INC_TABLE")
        base_table_name = element.get('TABLENAME')
        pk_external_table = element.get('PK_TABLE')
        iei_table_name = element.get('IEI_TABLE')
     
        # Checking the row counts of the BASE_TABLE against to be compared the PRIMARY_KEYS status json record counts
        if element.get('EXTRACT_TYPE') == 'PRIMARY_KEYS':
            
            # Collecting the intial IEI ROW Count 
            intial_IEI_row_count = self.SelectCountStarSQL(element, bq_project, bq_iei_dataset, iei_table_name)
            
            # Grabbing the PK Schema from the PK external table information schema 
            pk_table_schema_list = self.ExecuteInformationSchemaSQL(element, bq_project, bq_inc_external_dataset, pk_external_table)
            
            # Deleting IEIs that have been resolved, if the intial IEI Count is Zero this will be skipped 
            iei_del_transaction_status = self.ExecuteIEIDeleteSQL(element, pk_table_schema_list,bq_project, bq_basetable_source_dataset, base_table_name, bq_iei_dataset, iei_table_name, intial_IEI_row_count, bq_inc_external_dataset, pk_external_table)
            
            # Checking the IEI count post DEL. If the intial IEI count is 0, the DEL is skipped use the intial intial_IEI_row_count
            if intial_IEI_row_count == 0:
                pre_IEI_row_count = intial_IEI_row_count
            else:
                pre_IEI_row_count = self.SelectCountStarSQL(element, bq_project, bq_iei_dataset, iei_table_name)
            
            # Inserting the new IEIs    
            iei_insert_transaction_status = self.ExecuteIEIInsertSQL(element, pk_table_schema_list,bq_project, bq_basetable_source_dataset, base_table_name, bq_iei_dataset, iei_table_name, bq_inc_external_dataset, pk_external_table)

            # Returning the IEI row count after all transactions
            post_IEI_row_count = self.SelectCountStarSQL(element, bq_project, bq_iei_dataset, iei_table_name)
            
            # Summarizing the IEI Transaction Return Messages
            if iei_del_transaction_status == 'OK' and iei_insert_transaction_status == 'OK':
                iei_transaction_status = 'OK'
            elif iei_del_transaction_status != 'OK' and iei_insert_transaction_status == 'OK':
                iei_transaction_status = iei_del_transaction_status
            elif iei_del_transaction_status == 'OK' and iei_insert_transaction_status != 'OK':
                iei_transaction_status = iei_insert_transaction_status
            else:
                iei_transaction_status = 'NOT OK'
                            
            yield from self.ExecuteRowCountSQL(element, bq_project, bq_basetable_source_dataset, base_table_name, run_time, pk_external_table, bq_inc_external_dataset, pre_IEI_row_count, post_IEI_row_count, iei_transaction_status)
        
        # Checking the row counts of the INC_EXTERNAL_TABLE to be compared against the DATA status json record counts
        elif element.get('EXTRACT_TYPE') == 'DATA':
            yield from self.ExecuteRowCountSQL(element, bq_project, bq_inc_external_dataset, ext_data_table_name, run_time)
        
        else:
            logging.error("PIPELINE ERROR CODE HBIR7: NO SQL WAS EXECUTED")

    def ExecuteRowCountSQL(self, element, bq_project, dataset, table_name, run_time, pk_external_table = None, pk_dataset = None, iei_init_cnt = None, iei_final_cnt = None, iei_status = None):
        
        try:
            # Create a BigQuery client and defines a query
            client = bigquery.Client(project=bq_project)

            optional_pk_query_part_1 = ""
            optional_pk_query_part_2 = ""
            optional_pk_query_part_3 = ""
            
            if pk_external_table and pk_dataset:
                optional_pk_query_part_1 = f"""
                    ,distinct_row_count_tbl_2_cte AS (
                    -- OPTIONAL - Third CTE to get the distinct row count of the external pk table
                    SELECT 
                        COUNT(*) AS distinct_row_count
                    FROM (
                        SELECT 
                            DISTINCT *
                        FROM 
                            `{bq_project}.{pk_dataset}.{pk_external_table}`
                        ) AS distinct_combinations
                    )                    
                    ,row_count_tbl_2_cte AS (
                    -- OPTIONAL - Fourth CTE to get row count of the external pk table
                    SELECT
                        COUNT(*) AS row_count_2
                    FROM
                        `{bq_project}.{pk_dataset}.{pk_external_table}`
                    )
                """

                optional_pk_query_part_2 = """
                    ,r2.row_count_2 AS PK_ROW_COUNT
                    ,dr.distinct_row_count AS PK_DISTINCT_COUNT                
                """
                
                optional_pk_query_part_3 = """
                    CROSS JOIN
                    row_count_tbl_2_cte r2
                    CROSS JOIN
                    distinct_row_count_tbl_2_cte dr                
                """
            
            table_stats_query = f"""
                    WITH table_metadata AS (
                    -- First CTE to get table metadata from __TABLES__
                    SELECT
                        project_id AS BQ_PROJECT,
                        dataset_id AS BQ_DATASET,
                        table_id AS TABLENAME,
                        last_modified_time
                    FROM
                        `{bq_project}.{dataset}.__TABLES__`
                    WHERE
                        dataset_id = '{dataset}'
                        AND table_id = '{table_name}'
                    ),
                    row_count_cte AS (
                    -- Second CTE to get row count from the actual table
                    SELECT
                        COUNT(*) AS row_count
                    FROM
                        `{bq_project}.{dataset}.{table_name}`
                    )
                    {optional_pk_query_part_1}


                    SELECT
                    FORMAT_DATETIME('%F %T', DATETIME(TIMESTAMP_MILLIS(t.last_modified_time), 'America/Chicago')) AS LAST_MODIFIED,
                    t.BQ_PROJECT
                    ,t.BQ_DATASET
                    ,t.TABLENAME
                    ,r.row_count AS ROW_COUNT
                    {optional_pk_query_part_2}
                    FROM
                    table_metadata t
                    CROSS JOIN
                    row_count_cte r
                    {optional_pk_query_part_3}
            """
            retry_counter = 0
            result = None
            while retry_counter < 3:
                try:
                    # Execute the query and grab result
                    query_job = client.query(table_stats_query)
                    result = query_job.result()
                    break
                    
                except DeadlineExceeded as e:
                    wait_time = 5 ** retry_counter
                    logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to execute {element.get('EXTRACT_TYPE')} table BQ row count query for {element.get('PVO_NAME')}, {bq_project}.{dataset}.{table_name}\n{e}")                  
                    time.sleep(wait_time)
                    retry_counter += 1
                    
                except Exception as e:
                    raise(Exception(f"PIPELINE EXCEPTION: Failed to execute table BQ row count query - {e}"))
                
            if retry_counter >= 3:
                raise Exception(f"PIPELINE ERROR: MAX RETRIES")
            
            
            logging.info(f"PIPELINE INFO: Stage 7 - Executed BQ queries on: {bq_project}.{dataset}.{table_name}")
            # Adding the row counts if avaliable or logging an error if the table is empty
            if result is None:
                logging.error(f"PIPELINE ERROR: Failed to execute {element.get('EXTRACT_TYPE')} table BQ row count query for {element.get('PVO_NAME')}, {bq_project}.{dataset}.{table_name}")
            elif result.total_rows == 0:
                if element.get("TABLENAME") == "PVO NOT IN MAPPER":
                    temp_name = element.get("TABLENAME")
                else:
                    temp_name = "None"

                data_elements = {
                    'LAST_MODIFIED': "None", 
                    'BQ_PROJECT': bq_project, 
                    'BQ_DATASET': dataset,
                    'EXTRACT_TYPE': element.get('EXTRACT_TYPE'), 
                    'PVO_NAME': element.get('PVO_NAME'), 
                    'BICC_EXTRACT': element.get('BICC_EXTRACT'), 
                    'RECORD_COUNT': element.get('RECORD_COUNT'), 
                    'TABLENAME': temp_name,
                    'INC_TABLE': element.get('INC_TABLE'), 
                    "ROW_COUNT": -1, 
                    "RUN_TIME": run_time
                }
                logging.error(f"PIPELINE ERROR CODE HBIR6: ELEMENTS FROM BQ QUERY ON: {bq_project}.{dataset}.{table_name} - ROW_COUNT 0: {data_elements}")
                yield data_elements
            else:
                for row in result:
                    
                    # PK elements are different in that they contain the PK_ROW_COUNT and PK_DISTINCT_ROW_COUNT
                    if element.get('EXTRACT_TYPE') == 'PRIMARY_KEYS':
                        data_elements = {
                            'LAST_MODIFIED': row[0].replace(" ", "T"),
                            'BQ_PROJECT': row[1],
                            'BQ_DATASET': row[2],
                            'EXTRACT_TYPE': element.get('EXTRACT_TYPE'),
                            'PVO_NAME': element.get('PVO_NAME'),
                            'BICC_EXTRACT': element.get('BICC_EXTRACT'),
                            'RECORD_COUNT': element.get('RECORD_COUNT'),
                            'TABLENAME': element.get("TABLENAME"),
                            "ROW_COUNT": row[4],
                            'INC_TABLE': element.get('INC_TABLE'),
                            'PK_TABLE': element.get('PK_TABLE'),
                            'PK_ROW_COUNT': row[5],
                            'PK_DISTINCT_ROW_COUNT': row[6],
                            'IEI_TABLE' : element.get('IEI_TABLE'),
                            'INTIAL_IEI': iei_init_cnt,
                            'FINAL_IEI': iei_final_cnt,
                            'IEI_STATUS': iei_status,
                            "RUN_TIME": run_time
                        }
                    
                    elif element.get('EXTRACT_TYPE') == 'DATA':
                        
                        data_elements = {
                            'LAST_MODIFIED': row[0].replace(" ", "T"), 
                            'BQ_PROJECT': row[1], 
                            'BQ_DATASET': row[2],
                            'EXTRACT_TYPE': element.get('EXTRACT_TYPE'), 
                            'PVO_NAME': element.get('PVO_NAME'), 
                            'BICC_EXTRACT': element.get('BICC_EXTRACT'), 
                            'RECORD_COUNT': element.get('RECORD_COUNT'), 
                            'TABLENAME': element.get("TABLENAME"),
                            'INC_TABLE': element.get('INC_TABLE'),
                            "ROW_COUNT": row[4], 
                            "RUN_TIME": run_time
                        }
                        
                    yield data_elements
            
            client.close()
            return data_elements
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR7: Problem querying BQ for table row counts occurred. ERROR :- ", {e})

    def SelectCountStarSQL(self, element, bq_project, dataset,table):
        try:
            client = bigquery.Client(project=bq_project)
            
            information_schema_query = f"""
                SELECT COUNT(*) FROM `{bq_project}.{dataset}.{table}`;
            """
            
            retry_counter = 0
            result = None
            while retry_counter < 3:
                try:
                    # Execute the query and grab result
                    query_job = client.query(information_schema_query)
                    result = list(query_job.result())
                    return int(result[0][0])

                except DeadlineExceeded as e:
                    wait_time = 5 ** retry_counter
                    logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to execute IEI BQ row count query. {bq_project}.{dataset}.{table}\n{e}")                  
                    time.sleep(wait_time)
                    retry_counter += 1
                    
                except Exception as e:
                    raise(Exception(f"PIPELINE EXCEPTION: Failed to execute IEI BQ row count query - {e}"))

            if retry_counter >= 3:
                raise Exception(f"PIPELINE ERROR: MAX RETRIES")
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR7: Problem querying BQ for table information schema counts occurred. ERROR :- ", {e})
        
        finally:
            client.close()
                
    def ExecuteInformationSchemaSQL(self, element, bq_project, dataset, table):
        try:
            client = bigquery.Client(project=bq_project)
            
            information_schema_query = f"""
                SELECT column_name, data_type
                FROM `{bq_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table}'
                ORDER BY ordinal_position;
            """
            
            retry_counter = 0
            result = None
            while retry_counter < 3:
                try:
                    # Execute the query and grab result
                    query_job = client.query(information_schema_query)
                    result = list(query_job.result())
                    return [row[0] for row in result]

                except DeadlineExceeded as e:
                    wait_time = 5 ** retry_counter
                    logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to execute BQ PK information schema query. {bq_project}.{dataset}.{table}\n{e}")                  
                    time.sleep(wait_time)
                    retry_counter += 1
                    
                except Exception as e:
                    raise(Exception(f"PIPELINE EXCEPTION: Failed to execute table BQ PK information schema query - {e}"))

            if retry_counter >= 3:
                raise Exception(f"PIPELINE ERROR: MAX RETRIES")
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR7: Problem querying BQ for table information schema counts occurred. ERROR :- ", {e})
        
        finally:
            client.close()
            
    def ExecuteIEIDeleteSQL(self, element, schema_list, bq_project, base_table_dataset, basetable, iei_dataset, iei_table, iei_row_count, pk_dataset, pk_external_table):
        try:
            client = bigquery.Client(project=bq_project)

            # Don't delete IEIs as there are none.
            if iei_row_count == 0:
                
                # Skipping IEI delete as there are no IEIs from yesterday
                return 'OK'
            
            else:
                    
                sql_query = f"""
                DECLARE return_status STRING;
                DECLARE illegal_null_pks_not_exist BOOL;
                BEGIN
                    BEGIN TRANSACTION;
                    
                        -- CHECKING FOR ILLEGAL PKS/COMPOSITE KEYS (KEYS THAT MAY HAVE NULL VALUES)
                        SET illegal_null_pks_not_exist = NOT EXISTS(
                            SELECT 1 
                            FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pk
                            WHERE {" OR ".join([f"pk.{col} IS NULL" for col in schema_list])}
                        );                    
                
                
                        IF illegal_null_pks_not_exist THEN
                            -- DELETE IEIs that ARE in the base table
                            DELETE FROM `{bq_project}.{iei_dataset}.{iei_table}` ieit
                                WHERE EXISTS(
                                SELECT 1
                                FROM `{bq_project}.{base_table_dataset}.{basetable}` bt
                                WHERE {" AND ".join([f"bt.{col} = ieit.{col}" for col in schema_list])}
                            );
                            SET return_status = 'OK';
                        END IF;

                        IF NOT illegal_null_pks_not_exist THEN
                            -- DELETE IEIs that ARE in the base table (KEYS THAT MAY HAVE NULL VALUES)
                            DELETE FROM `{bq_project}.{iei_dataset}.{iei_table}` ieit
                                WHERE EXISTS(
                                SELECT 1
                                FROM `{bq_project}.{base_table_dataset}.{basetable}` bt
                                WHERE {" AND ".join([f"IFNULL(SAFE_CAST(bt.{col} AS STRING),'') = IFNULL(SAFE_CAST(ieit.{col} AS STRING),'')" for col in schema_list])} 
                            );                
                            SET return_status = 'OK - PK HAS NULLs';                    
                        END IF;                    
                                
                        IF illegal_null_pks_not_exist THEN
                            -- DELETE IEIs that ARE NOT in the PK TABLE
                            DELETE FROM `{bq_project}.{iei_dataset}.{iei_table}` ieit
                                WHERE NOT EXISTS(
                                SELECT 1
                                FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pkt
                                WHERE {" AND ".join([f"pkt.{col} = ieit.{col}" for col in schema_list])}
                            );
                            SET return_status = 'OK';
                        END IF;
                        
                        IF NOT illegal_null_pks_not_exist THEN
                            -- DELETE IEIs that ARE NOT in the PK TABLE (KEYS THAT MAY HAVE NULL VALUES)
                            DELETE FROM `{bq_project}.{iei_dataset}.{iei_table}` ieit
                                WHERE NOT EXISTS(
                                SELECT 1
                                FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pkt
                                WHERE {" AND ".join([f"IFNULL(SAFE_CAST(pkt.{col} AS STRING),'') = IFNULL(SAFE_CAST(ieit.{col} AS STRING),'')" for col in schema_list])}
                            );                
                            SET return_status = 'OK - PK HAS NULLs';                    
                        END IF;   
                        
                        IF return_status IS NULL THEN
                            SET return_status = 'SKIPPED';
                        END IF;
                        
                    COMMIT TRANSACTION;
                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    SET return_status = CONCAT('IEI delete transactions failed at', CURRENT_TIMESTAMP(),' Error: ',@@error.message);
                    ROLLBACK TRANSACTION;   
                END;
                
                SELECT return_status;
                """

            retry_counter = 0
            result = None
            while retry_counter < 3:
                try:
                    # Execute the query and grab result
                    query_job = client.query(sql_query)
                    result = list(query_job.result())
                    transaction_status = result[0][0]
                
                    if "OK" in transaction_status:
                        return 'OK'
                    else:
                        raise Exception(f"{transaction_status} {element.get('PVO_NAME')}")
                    
                except DeadlineExceeded as e:
                    wait_time = 5 ** retry_counter
                    logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to execute BQ IEI DELETE query. {bq_project}.{iei_dataset}.{iei_table}\n{e}")                  
                    time.sleep(wait_time)
                    retry_counter += 1
                    
                except Exception as e:
                    raise(Exception(f"PIPELINE EXCEPTION: Failed to execute BQ IEI DELETE query - {e}"))

            if retry_counter >= 3:
                raise Exception(f"PIPELINE ERROR: MAX RETRIES")
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR7: Problem executing IEI delete transaction. ERROR :- ", {e})
        
        finally:
            client.close()

    def ExecuteIEIInsertSQL(self, element, schema_list, bq_project, base_table_dataset, basetable, iei_dataset, iei_table, pk_dataset, pk_external_table):
        try:
            client = bigquery.Client(project=bq_project)
            
            sql_query = f"""
            DECLARE return_status STRING;
            DECLARE illegal_null_pks_not_exist BOOL;            
            BEGIN
                BEGIN TRANSACTION;

                    -- CHECKING FOR ILLEGAL PKS/COMPOSITE KEYS (KEYS THAT MAY HAVE NULL VALUES)
                    SET illegal_null_pks_not_exist = NOT EXISTS(
                        SELECT 1 
                        FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pk
                        WHERE {" OR ".join([f"pk.{col} IS NULL" for col in schema_list])}
                    );
            
                    IF illegal_null_pks_not_exist THEN
                        -- Inserting new IEIs into the IEI table
                        INSERT INTO `{bq_project}.{iei_dataset}.{iei_table}` (
                            {", ".join(schema_list)}
                        )
                        SELECT 
                            {", ".join([f"pkt.{col}" for col in schema_list])}
                        FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pkt
                        WHERE NOT EXISTS(
                            SELECT 1
                            FROM `{bq_project}.{base_table_dataset}.{basetable}` bt
                            WHERE {" AND ".join([f"pkt.{col} = bt.{col}" for col in schema_list])}
                        );
                        SET return_status = 'OK';
                    END IF;            
            
                    IF NOT illegal_null_pks_not_exist THEN
                        -- Inserting new IEIs into the IEI table (KEYS THAT MAY HAVE NULL VALUES)
                        INSERT INTO `{bq_project}.{iei_dataset}.{iei_table}` (
                            {", ".join(schema_list)}
                        )
                        SELECT 
                            {", ".join([f"pkt.{col}" for col in schema_list])}
                        FROM `{bq_project}.{pk_dataset}.{pk_external_table}` pkt
                        WHERE NOT EXISTS(
                            SELECT 1
                            FROM `{bq_project}.{base_table_dataset}.{basetable}` bt
                            WHERE {" AND ".join([f"IFNULL(SAFE_CAST(pkt.{col} AS STRING),'') = IFNULL(SAFE_CAST(bt.{col} AS STRING),'')" for col in schema_list])}
                        );                                      
                        SET return_status = 'OK - PK HAS NULLs';                
                    END IF;            
                    
                    IF return_status IS NULL THEN
                        SET return_status = 'SKIPPED';
                    END IF;
                                    
                COMMIT TRANSACTION;
            EXCEPTION WHEN ERROR THEN
                SELECT @@error.message;
                SET return_status = CONCAT('IEI insert transactions failed at', CURRENT_TIMESTAMP(),' Error: ',@@error.message);
                ROLLBACK TRANSACTION;   
            END;
            
            SELECT return_status;
            """

            retry_counter = 0
            result = None
            while retry_counter < 3:
                try:
                    # Execute the query and grab result
                    query_job = client.query(sql_query)
                    result = list(query_job.result())
                    transaction_status = result[0][0]
                
                    if 'OK' in transaction_status:
                        return 'OK'
                    else:
                        raise Exception(f"{transaction_status} - {element.get('PVO_NAME')}")
                    
                except DeadlineExceeded as e:
                    wait_time = 5 ** retry_counter
                    logging.warning(f"PIPELINE WARNING: Attempt #{retry_counter} failed RETRYING in {wait_time}s to execute BQ IEI INSERT query. {bq_project}.{iei_dataset}.{iei_table}\n{e}")                  
                    time.sleep(wait_time)
                    retry_counter += 1
                    
                except Exception as e:
                    raise(Exception(f"PIPELINE EXCEPTION: Failed to execute BQ IEI INSERT query - {e}"))
                    
            if retry_counter >= 3:
                raise Exception(f"PIPELINE ERROR: MAX RETRIES")
        
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR7: Problem executing IEI insert transaction. ERROR :- ", {e})
        
        finally:
            client.close()

                        
class CombineDataPkElements(beam.DoFn):
    '''
    Ingesting elements and yielding as a generator, logging ingestion. 
    Args:   Element: FileMetadata Objects 
    Yields: Element: Generator
    '''
    def process(self, element):
        
        try:
            
            pvo_name, records = element
            
            pk_record = None
            data_record = None
            
            for record in records:
                
                if record.get('EXTRACT_TYPE') == 'DATA':
                    data_record = record
                elif record.get('EXTRACT_TYPE') == 'PRIMARY_KEYS':
                    pk_record = record
                
            if data_record and pk_record:
                logging.info(f"PIPELINE INFO: Combining PVO_NAME {pvo_name}")
                
                # EXT = Inc External Tables == Data Status Json
                # BS = Base Tables = Primary Key Status Json
                combined_elements = {
                    'EXT_LAST_MODIFIED' : data_record.get('LAST_MODIFIED'),
                    'EXT_BQ_PROJECT' : data_record.get('BQ_PROJECT'),
                    'EXT_BQ_DATASET' : data_record.get('BQ_DATASET'),
                    'EXT_EXTRACT_TYPE' : data_record.get('EXTRACT_TYPE'),
                    'EXT_PVO_NAME' : data_record.get('PVO_NAME'),
                    'EXT_BICC_EXTRACT' : data_record.get('BICC_EXTRACT'),
                    'EXT_RECORD_COUNT' : data_record.get('RECORD_COUNT'),
                    'EXT_INC_TABLE' : data_record.get('INC_TABLE'),
                    'EXT_ROW_COUNT' : data_record.get('ROW_COUNT'),
                    'EXT_RUN_TIME' : data_record.get('RUN_TIME'),
                    'BS_LAST_MODIFIED' : pk_record.get('LAST_MODIFIED'),
                    'BS_BQ_PROJECT' : pk_record.get('BQ_PROJECT'),
                    'BS_BQ_DATASET' : pk_record.get('BQ_DATASET'),
                    'BS_EXTRACT_TYPE' : pk_record.get('EXTRACT_TYPE'),
                    'BS_PVO_NAME' : pk_record.get('PVO_NAME'),
                    'BS_BICC_EXTRACT' : pk_record.get('BICC_EXTRACT'),
                    'BS_RECORD_COUNT' : pk_record.get('RECORD_COUNT'),
                    'BS_TABLENAME' : pk_record.get('TABLENAME'),
                    'BS_ROW_COUNT' : pk_record.get('ROW_COUNT'),
                    'PK_TABLE': pk_record.get('PK_TABLE'),
                    'PK_ROW_COUNT' : pk_record.get('PK_ROW_COUNT'),
                    'PK_DISTINCT_ROW_COUNT' : pk_record.get('PK_DISTINCT_ROW_COUNT'),
                    'BS_RUN_TIME' : pk_record.get('RUN_TIME'),
                    'IEI_TABLE' : pk_record.get('IEI_TABLE'),
                    'INTIAL_IEI': pk_record.get('INTIAL_IEI'),
                    'FINAL_IEI': pk_record.get('FINAL_IEI'),
                    'IEI_STATUS': pk_record.get('IEI_STATUS')
                    }
                
                yield combined_elements
            elif not pk_record:
                logging.error(f"PIPELINE ERROR CODE HBIR10: Primary Key JSON is missing {pvo_name} \n element {element}")
            elif not data_record:
                logging.error(f"PIPELINE ERROR CODE HBIR10: Data JSON is missing {pvo_name} \n element {element}")
                
        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR10: Problem combining Data and PK row counts in CombineDataPkElements occurred. Element - {element} ERROR :- ", {e})
            
class CheckingRowCounts(beam.DoFn):
    '''
    This class is the code that actually does the comparison logic of status file record count vs the respective table's row count
    Args:   Element: Fully Hydrated Combined Generator 
    Returns:
    '''

    def process(self, element):
        try:
            
            pk_status_json_record_count = int(element['BS_RECORD_COUNT']) # PK STATUS FILE RECORD COUNT
            base_row_count = int(element['BS_ROW_COUNT']) # BASE TABLE ROW COUNT
            
            ext_record_count = int(element['EXT_RECORD_COUNT']) # EXT DATA TABLE RECORD COUNT
            ext_row_count = int(element['EXT_ROW_COUNT']) # EXT DATA TABLE ROW COUNT 
            
            external_pk_table_count = int(element.get('PK_ROW_COUNT')) # EXT PK TABLE ROW COUNT
            external_pk_table_distinct_count = int(element.get('PK_DISTINCT_ROW_COUNT')) # EXT PK TABLE ROW DISTINCT COUNT
            
            iei_init_count = int(element.get('INTIAL_IEI')) # IEI AFTER DELS
            iei_final_count = int(element.get('FINAL_IEI')) # IEI COUNT AFTER INSERTS
            iei_status = element.get('IEI_STATUS') # IEI TRANSACTION COMPLETE? "OK"
                        
            # DEFAULT VALUES
            EXT_STATUS = "ERROR"
            EXT_REASON = "UNKNOWN"
            
            # CHECKING EXTERNAL DATA TABLES
            # Checking the row counts of the INC_EXTERNAL_TABLE against the DATA status json record counts
            # Styling comment - Typicall UPPER CASE variables are global in python. These are capital to match the styling of the KEYs in the elements. 
            if ext_record_count == ext_row_count:
                EXT_STATUS = "SUCCESS"
                EXT_REASON = "NULL"
            elif ext_record_count > ext_row_count:
                EXT_REASON = f"BAD EXT DATA TABLE: Ext Data Table Row Cnt ({ext_row_count}) less than Status File ({ext_record_count})"
            elif ext_record_count < ext_row_count:
                EXT_REASON = f"BAD EXT DATA TABLE: Ext Data Table Row Cnt ({ext_row_count}) greater than Status File ({ext_record_count})"

            # CHECK EXTERNAL PK TABLE AND BASE TABLES
            # Checking the row counts of the BASE_TABLE against the PRIMARY_KEYS status json record counts
            
            # DEFAULT VALUES
            BS_STATUS = "ERROR"
            BS_REASON = "UNKNOWN"
            
            # The table is OK!
            # BaseTable PKs == PK Status JSON && PK External Table == PK Status JSON  && IEI after DELs should always be 0
            if base_row_count == (pk_status_json_record_count) and external_pk_table_count == pk_status_json_record_count and iei_init_count == 0 and iei_final_count == 0:
                BS_STATUS = "SUCCESS"
                BS_REASON = "NULL"

            # Checking if there are duplicate primary keys
            elif external_pk_table_distinct_count < external_pk_table_count:
                BS_REASON = f"DUPLICATE PKs IN PK EXT TBL: in PK Ext TBl ({element.get('PK_TABLE')}) (Dstnct Cnt:{external_pk_table_distinct_count} Cnt:{external_pk_table_count})"
            
            # Checking if there are unresolved OLD IEIs from yesterday
            elif iei_init_count > 0: 
                BS_REASON = f"THERE ARE UNRESOLVED IEIs FROM YESTERDAY - REQUEST A FULL EXTRACT"
            
            # There are NEW IEIs in the PK extra
            elif pk_status_json_record_count - iei_final_count - base_row_count == 0 and external_pk_table_count == pk_status_json_record_count:                
                BS_STATUS = "SUCCESS"
                BS_REASON = "IEIs PRESENT"
                
            # There records in the basetable that didn't get cleanedup
            elif (pk_status_json_record_count - iei_final_count) < base_row_count and external_pk_table_count == pk_status_json_record_count:
                BS_REASON = f"EXTRA BASE TBL RECORDS: Base Table Row Cnt ({base_row_count}) GREATER than PK Status File ({pk_status_json_record_count}) (NO Duplicate PKs, PK External Table OK) - PK cleanup may have failed"
            
            else:
                BS_REASON = f"UNKNOWN ERROR: Base Table Row Cnt ({base_row_count}), PK Status File ({pk_status_json_record_count}), Ext PK Row Cnt({external_pk_table_count}), Dstnct Ext Pk Row Ctn ({external_pk_table_distinct_count}), IEI Init Cnt ({iei_init_count}), IEI Final Cnt ({iei_final_count})"
            
            # Returning the dictionary to be written to BQ
            # Avaliable Elements:
                # 'EXT_LAST_MODIFIED' - The time the incremental data external table was updated
                # 'EXT_BQ_PROJECT' - Project ID, should be same as base table project ID
                # 'EXT_BQ_DATASET' - This is the Dump dataset (eg. int_bicc_dump_us_d)
                # 'EXT_EXTRACT_TYPE' - DATA 
                # 'EXT_PVO_NAME' - PVO Name
                # 'EXT_BICC_EXTRACT' - Extract name from the mapper file
                # 'EXT_RECORD_COUNT' - Record Count from the data status json file
                # 'EXT_INC_TABLE' - This is the external data table name. INC_<base_table_name> 
                # 'EXT_ROW_COUNT' - This is the row count of the external data table name. INC_<base_table_name> 
                # 'EXT_RUN_TIME' - This is the runtime of the recon pipeline
                # 'EXT_STATUS' - This is the status of the above record and row count comparison
                # 'EXT_REASON' - This is the reason code of the record and row count comparison
                # 'BS_LAST_MODIFIED' - The time the base table was updated
                # 'BS_BQ_PROJECT' - Project ID, should be same as external data table project ID
                # 'BS_BQ_DATASET' - This is the Base Table dataset (eg. int_bicc_data_us_d)
                # 'BS_EXTRACT_TYPE' - PRIMARY_KEYS
                # 'BS_PVO_NAME' - PVO Name
                # 'BS_BICC_EXTRACT' - Extract name from the mapper file
                # 'BS_RECORD_COUNT' - Record Count from the primary key status json file
                # 'BS_TABLENAME' - This is the base table name. <base_table_name> 
                # 'BS_ROW_COUNT' - This is the row count of the base table
                # 'BS_RUN_TIME' - This is the runtime of the recon pipeline
                # 'BS_STATUS' - This is the status of the above record and row count comparison
                # 'BS_REASON' - This is the reason code of the record and row count comparison
                # 'PK_TABLE' - This is the primary key table external table name from the CSV mapper table only avaliable in PK elements
                # 'PK_ROW_COUNT'  - This is the row count of the primary key table external table name only avaliable in PK elements
                # 'PK_DISTINCT_ROW_COUNT'  - This is the distinct row count of the primary key table external table name only avaliable in PK elements
                # 'IEI_TABLE' -  This is the IEI table name from the CSV mapper table only avaliable in PK elements
                # 'INTIAL_IEI' - This is the intial count of the IEIs after DELETEs or 0 if there are no IEIs
                # 'FINAL_IEI' - This is the final count of the IEIs after INSERTS
                # 'IEI_STATUS' - This is the status of the IEI DELETE and INSERT transactions from earlier in the RECON pipeline

            data_elements = {
                'PVO_NAME':element.get('EXT_PVO_NAME'), # This is the same from EXT_ and BS_
                'BICC_EXTRACT':element.get('EXT_BICC_EXTRACT'), # This is the same from EXT_ and BS_
                'BQ_PROJECT':element.get('EXT_BQ_PROJECT'), # This is the same from EXT_ and BS_
                'RECON_RUN_TIME':element.get('EXT_RUN_TIME'), # This is the same from EXT_ and BS_
                
                'EXT_EXTRACT_TYPE': element.get('EXT_EXTRACT_TYPE'),
                'EXT_DATA_STATUS_JSON_RECORD_COUNT':element.get('EXT_RECORD_COUNT'),
                'EXT_BQ_DATASET':element.get('EXT_BQ_DATASET'),
                'EXT_DATA_TABLE':element.get('EXT_INC_TABLE'),
                'EXT_DATA_TABLE_ROW_COUNT':element.get('EXT_ROW_COUNT'),
                'EXT_DATA_TABLE_LAST_MODIFIED_TIME':element.get('EXT_LAST_MODIFIED'),
                'EXT_STATUS':EXT_STATUS,
                'EXT_REASON':EXT_REASON,
                
                'BS_EXTRACT_TYPE':element.get('BS_EXTRACT_TYPE'),
                'BS_PK_STATUS_JSON_RECORD_COUNT':element.get('BS_RECORD_COUNT'),
                'BS_BQ_DATASET':element.get('BS_BQ_DATASET'),
                'BS_TABLENAME':element.get('BS_TABLENAME'),
                'BS_TABLE_ROW_COUNT':element.get('BS_ROW_COUNT'),
                'BS_TABLE_LAST_MODIFIED_TIME':element.get('BS_LAST_MODIFIED'),
                'IEI_COUNT': iei_final_count,  
                'BS_STATUS':BS_STATUS,
                'BS_REASON':BS_REASON
            }
            
            if BS_STATUS == "SUCCESS" and EXT_STATUS == "SUCCESS" and BS_REASON == "NULL" and EXT_REASON == "NULL":
                logging.info(f"PIPELINE INFO: Stage 10 PVO-{data_elements.get('PVO_NAME')} Basetable OK & External Table OK - Completed recon of row vs record counts for DATA and PRIMARY_KEYS.")
            elif BS_STATUS == "SUCCESS" and EXT_STATUS == "SUCCESS" and BS_REASON != "NULL":
                logging.info(f"PIPELINE INFO: Stage 10 PVO-{data_elements.get('PVO_NAME')} Basetable OK & External Table OK - Completed recon of row vs record counts. {BS_REASON}")
            elif BS_STATUS != "SUCCESS" and EXT_STATUS == "SUCCESS":
                logging.warning(f"PIPELINE WARNING: Stage 10 PVO-{data_elements.get('PVO_NAME')} Basetable NOTOK & External Table OK - {BS_REASON}")
            elif BS_STATUS == "SUCCESS" and EXT_STATUS != "SUCCESS":
                logging.warning(f"PIPELINE WARNING: Stage 10 PVO-{data_elements.get('PVO_NAME')} Basetable OK & External Table NOTOK - {EXT_REASON}")
            else:
                logging.warning(f"PIPELINE WARNING: Stage 10 PVO-{data_elements.get('PVO_NAME')} Basetable NOTOK & External Table NOTOK - {EXT_REASON} - {BS_REASON}")
            
            yield data_elements

        except Exception as e:
            logging.error(f"PIPELINE ERROR CODE HBIR10: Problem checking row counts in CheckingRowCounts occurred. ERROR :- ", {e})

def run_pipeline(zipped_bucket, json_prefix, csv_map_bucket, csv_map_file, csv_prefix, bq_project, bq_basetable_source_dataset, bq_inc_external_dataset, bq_iei_dataset, bq_target_recon_dataset, bq_table_base_recon, extract):
    
    logging.getLogger("apache_beam.transforms.core").setLevel(logging.ERROR)

    options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    logging.info(f"PIPELINE INFO: ##################### Initiating Recon Pipeline  ##################### \nwith parameters:\n--zipped_bucket={zipped_bucket}\n --json_prefix={json_prefix}\n --csv_map_bucket={csv_map_bucket}\n --csv_map_file={csv_map_file}\n --csv_prefix={csv_prefix}\n --bq_project={bq_project}\n --bq_basetable_source_dataset={bq_basetable_source_dataset}\n --bq_inc_external_dataset={bq_inc_external_dataset}\n --bq_target_recon_dataset={bq_target_recon_dataset}\n --bq_table_base_recon={bq_table_base_recon}\n --extract={extract}\n")

    # Creating runtime
    utc_now = datetime.now(timezone.utc)
    run_time = utc_now.strftime('%Y-%m-%dT%H:%M:%S') + f".{utc_now.microsecond:06d}Z"
    
    logging.info("PIPELINE INFO: ********* Starting Pipeline Stage 1 Starting: List Files *********")
    with beam.Pipeline(options=options) as p:
        (p | "List Files" >> fileio.MatchFiles(f'gs://{zipped_bucket}/{json_prefix}/{extract}*.JSON', empty_match_treatment=fileio.EmptyMatchTreatment.DISALLOW)
                | "Logging Ingested Files" >> beam.ParDo(LogIngestedFiles())
                | "Read Files" >> fileio.ReadMatches()
                | "Extract JSON Data" >> beam.ParDo(ReadJSON())
                | "Shuffle" >> beam.Reshuffle() # Reshuffling as there is a fanout in the prior step (Number of tables in status files)
                | "Check against Mapper File" >> beam.ParDo(CheckMapperCSV(), csv_map_bucket, csv_prefix, csv_map_file)
                | "Verify BQ table" >> beam.ParDo(QueryBQForTable(), bq_project, bq_basetable_source_dataset, bq_inc_external_dataset, bq_iei_dataset, run_time)
                | "Group by PVO_NAME combine in Group" >> beam.GroupBy(lambda x:x['PVO_NAME'])  # Key by Basetable AKA Pvo
                | "Combining Data and PK dictionary in element" >> beam.ParDo(CombineDataPkElements())
                | "Performing Row Count Validation" >> beam.ParDo(CheckingRowCounts())
                | "Write Inc Ext Table Stats to BQ" >> beam.io.WriteToBigQuery(
                            table=f'{bq_project}.{bq_target_recon_dataset}.{bq_table_base_recon}',
                            schema='PVO_NAME:STRING,BICC_EXTRACT:STRING,BQ_PROJECT:STRING,RECON_RUN_TIME:TIMESTAMP,EXT_EXTRACT_TYPE:STRING,EXT_DATA_STATUS_JSON_RECORD_COUNT:INTEGER,EXT_BQ_DATASET:STRING,EXT_DATA_TABLE:STRING,EXT_DATA_TABLE_ROW_COUNT:INTEGER,EXT_DATA_TABLE_LAST_MODIFIED_TIME:TIMESTAMP,EXT_STATUS:STRING,EXT_REASON:STRING,BS_EXTRACT_TYPE:STRING,BS_PK_STATUS_JSON_RECORD_COUNT:INTEGER,BS_BQ_DATASET:STRING,BS_TABLENAME:STRING,BS_TABLE_ROW_COUNT:INTEGER,BS_TABLE_LAST_MODIFIED_TIME:TIMESTAMP,IEI_COUNT:INTEGER,BS_STATUS:STRING,BS_REASON:STRING',
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            )
        )
       
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    logging.info("PIPELINE INFO: HELLO STARTING THE INC PIPELINE RECON JOB")

    try:

        parser = argparse.ArgumentParser(description='Data Recon DataFlow program')
        parser.add_argument('--bq_project', type=str, required=True, help='BigQuery Project ID')
        parser.add_argument('--bq_basetable_source_dataset', type=str, required=True, help='BigQuery Dataset for Basetable Source tables')
        parser.add_argument('--bq_inc_external_dataset', type=str, required=True, help='BigQuery Dataset for Incremental Data External tables')
        parser.add_argument('--bq_target_recon_dataset', type=str, required=True, help='BigQuery Dataset for Target table')
        parser.add_argument('--bq_table_base_recon', type=str, required=True, help='Reconcile metadata for base tables')
        parser.add_argument('--zipped_bucket', type=str, required=True, help='Name of the top level GCS bucket where JSON and Mapping file is stored')
        parser.add_argument('--json_prefix', type=str, required=True, help='Prefix for json status files. The subfolder path in GCS bucket. Do not include / at the beginning or end of the path')
        parser.add_argument('--csv_map_bucket', type=str, required=True, help='Name of the top level GCS bucket where JSON and Mapping file is stored')
        parser.add_argument('--csv_prefix', type=str, required=False, default="", help='Prefix for csv file. The subfolder path in GCS bucket. Do not include / at the beginning or end of the path')
        parser.add_argument('--csv_map_file', type=str, required=True, help='CSV mapper file')
        parser.add_argument('--extract_name', type=str, required=False, default="", help='Prefix for the dataset being loaded')

        args, pipeline_args = parser.parse_known_args()

        # Checking the pipeline arguments
        if "/" in args.zipped_bucket:
            raise argparse.ArgumentTypeError(f"--zipped_bucket: {args.zipped_bucket} contains "/" or subpath must be top level storage bucket")
        if "/" in args.csv_map_bucket:
            raise argparse.ArgumentTypeError(f"--csv_map_bucket: {args.csv_map_bucket} contains "/" or subpath must be top level storage bucket")
        if args.json_prefix.startswith("/") or args.json_prefix.endswith("/"):
            raise argparse.ArgumentTypeError(f"--json_prefix: {args.json_prefix} starts or ends with "/". May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
        if args.csv_prefix.startswith("/") or args.csv_prefix.endswith("/"):
            raise argparse.ArgumentTypeError(f"--csv_prefix: {args.csv_prefix} starts or ends with "/". May contain / in the middle of the subpath e.g. (folder1/folder2/folder3)")
        if args.extract_name.islower():
            extract_name = args.extract_name.upper()
            logging.warning(f"PIPELINE WARNING: --extract_name was passed as a lower {args.extract_name}. It should be capitalized (i.e. FSC_LONG2)")
        elif "-" in args.extract_name:
            extract_name = args.extract_name.upper().replace('-','_')
            logging.warning(f"PIPELINE WARNING: --extract_name was passed with - instead of _ {args.extract_name}.")
        else:
            extract_name = args.extract_name
        
        # Using the reconcilition dataset to house the IEI tables
        bq_iei_dataset = args.bq_target_recon_dataset
        
        run_pipeline(args.zipped_bucket, args.json_prefix,args.csv_map_bucket, args.csv_map_file, args.csv_prefix, args.bq_project, args.bq_basetable_source_dataset, args.bq_inc_external_dataset, bq_iei_dataset, args.bq_target_recon_dataset, args.bq_table_base_recon, extract_name)
    except fileio.filesystem.BeamIOError as e:
            logging.error(f"PIPELINE ERROR CODE HBIR1: PIPELINE STAGE 1 'List Files' or PIPELINE STAGE 3 'Read Files'. {e}")
            
    except Exception as e:
            raise TypeError(f"PIPELINE FATAL ERROR CODE HBIR99: in __main__ , Please Investigate :- {e}")