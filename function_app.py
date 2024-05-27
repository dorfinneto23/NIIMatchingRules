import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 
import csv #helping convert json to csv
from io import StringIO  # in order for merge_csv_rows_by_diagnosis function 
from collections import defaultdict # in order for merge_csv_rows_by_diagnosis function 
from openai import OpenAI # in order to use openai asistant 
import time  # Import the time module
import openai


# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#openai key
openai_key = os.environ.get('openai_key')


# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'

# Set your OpenAI API key
#openai.api_key = os.environ.get('openai_key') 


#Asistant request 
def assistant_request(csv_string, assistant_id, vector_store_id):
    # Read CSV string into DataFrame
    #csv_data = csv.DictReader(io.StringIO(csv_string))
    #openai 
    client  = OpenAI(api_key=openai_key)

    # For demonstration, let's assume we want to summarize the data
    # Convert DataFrame to a string in a readable format
    #data_summary = csv_data.describe().to_string()
    data_summary = StringIO(csv_string)

    # Create the request content for the assistant
    content = data_summary


    # Run the assistant with create_and_run
    run = client.beta.threads.create_and_run(
        assistant_id=assistant_id,
        tools=[{"type": "file_search"}],
        tool_resources={
            "file_search": {
                "vector_store_ids": [vector_store_id]
            }
        },
        thread={
            "messages": [
                {"role": "user", "content": content},
            ]
        }
    )

    # Wait for the run to complete
    while run.status in ['queued', 'in_progress']:
        time.sleep(1)  # Pause for a second before checking the status again
        run = client.beta.threads.runs.retrieve(
            thread_id=run.thread_id,#added
            run_id=run.id
        )

    # Get the response text from the assistant
    messages = client.beta.threads.messages.list(
        thread_id=run.thread_id,
        order="asc"
    )

    assistant_response = messages.data[-1].content
    return assistant_response


#get content from storage table 
def get_content_Csv(table_name, partition_key, row_key):
    """
    Retrieve the 'contentAnalysisCsv' field from the specified Azure Storage Table.

    :param table_name: Name of the table.
    :param partition_key: PartitionKey of the entity.
    :param row_key: RowKey of the entity.
    :param connection_string: Connection string for the Azure Storage account.
    :return: The value of the 'contentAnalysisCsv' field or None if not found.
    """
    try:
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the value of 'contentAnalysisCsv' field
        encoded_content_csv = entity.get('contentCsvConsolidation')
        retrieved_csv = encoded_content_csv.replace('\\n', '\n') 
        logging.info(f"contentCsvConsolidation: {retrieved_csv}")
        return retrieved_csv
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    



app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="niimatchingrules",
                               connection="medicalanalysis_SERVICEBUS") 
def NIIMatchingRules(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    clinicArea = message_data_dict['clinicArea']
    storageTable = message_data_dict['storageTable']
    content_csv = get_content_Csv(storageTable, caseid, clinicArea)
    logging.info(f"storageTable: {storageTable},caseid: {caseid},clinicArea: {clinicArea}")
    ass_result = assistant_request(content_csv, "asst_3nZCjLaXe06CvPR5L05gkGxk", "vs_cca6GF9kkzlu7XlHEg6yCYV5")
    logging.info(f"ass_result: {ass_result}")