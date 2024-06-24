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
import re # for removal disabilities 0% filter_assistantResponse


# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#Assistant openai key
openai_key = os.environ.get('openai_key')


# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'



#save assistantResponse content 
def save_assistantResponse(content,caseid,folder,filename):
    try:
        logging.info(f"save_ContentByClinicAreas start, content: {content},caseid: {caseid},filename: {filename}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/{folder}/{filename}"
        # Upload the blob and overwrite if it already exists
        blob_client = container_client.upload_blob(name=destinationPath, data=content, overwrite=True)
        logging.info(f"the ContentByClinicAreas content file url is: {blob_client.url}")
        return destinationPath
    
    except Exception as e:
        print("An error occurred:", str(e))

# get content csv from azure storage 
def get_contentcsv_from_storage(path):
    try:
        logging.info(f"get_contentcsv function strating, path value: {path}")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"get_contentcsv: data from the txt file is {filecontent}")
        retrieved_csv = filecontent.replace('\\n', '\n') 
        return retrieved_csv
    except Exception as e:
        logging.error(f"get_contentcsv: Error update case: {str(e)}")
        return None    

    
#  Function filers paragraphs where the disability percentage is not 0% by pattern
def filter_assistantResponse_v2(assistantResponse):
    
    try:
        logging.info(f"filter_assistantResponse_v2: assistantResponse value: {assistantResponse}")
        # Regex pattern to match the blocks
        pattern = r'\{[^}]*\}'
        
        # Find all blocks
        blocks = re.findall(pattern, assistantResponse)
        
        # Filter out blocks where **Disability Percentage** is 0%
        filtered_blocks = [
            block for block in blocks 
            if '**Disability Percentage:** 0%' not in block
        ]
        
        # Join the filtered blocks into a single string
        result = '\n\n'.join(filtered_blocks)
        #cleaning not relevant signs in the text
        logging.info(f"filter_assistantResponse_v2: result value: {result}")
        result_clean = result.replace("{", "").replace("}", "")
        logging.info(f"filter_assistantResponse_v2: result_clean value: {result_clean}")
        return result_clean
    except Exception as e:
        return f"{str(e)}"  

#  Function filers paragraphs where the disability percentage is 0% by pattern
def get_assistantResponse_no_Disabilities(assistantResponse):
    try:
        # Regex pattern to match the blocks
        pattern = r'\{[^}]*\}'
        
        # Find all blocks
        blocks = re.findall(pattern, assistantResponse)
        
        # Filter to keep only blocks where **Disability Percentage** is 0%
        filtered_blocks = [
            block for block in blocks 
            if '**Disability Percentage:** 0%' in block
        ]
        
        # Join the filtered blocks into a single string
        result = '\n\n'.join(filtered_blocks)
        
        # Clean the text by removing curly braces
        result_clean = result.replace("{", "").replace("}", "")
        
        return result_clean
    except Exception as e:
        return str(e)

# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value,field2,value2):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # update case
        cursor.execute(f"UPDATE cases SET {field} = ?,{field2} = ? WHERE id = ?", (value,value2, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value} and field name: {field2} , value: {value2}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    

#  Function check how many rows in partition of azure storage 
def count_rows_in_partition( table_name,partition_key):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    
    # Define the filter query to count entities with the specified partition key and where contentAnalysisCsv is not null or empty
    filter_query = f"PartitionKey eq '{partition_key}'"
    
    # Query the entities and count the number of entities
    entities = table_client.query_entities(query_filter=filter_query)
    count = sum(1 for _ in entities)  # Sum up the entities
    
    if count>0:
        return count
    else:
        return 0
    
#  Function check how many rows in partition of azure storage table where status = 4 (assistant response done done)
def count_rows_status_done ( table_name,partition_key):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    
    # Define the filter query to count entities with the specified partition key and where contentAnalysisCsv is not null or empty
    filter_query = f"PartitionKey eq '{partition_key}' and status eq 6"
    
    # Query the entities and count the number of entities
    entities = table_client.query_entities(query_filter=filter_query)
    count = sum(1 for _ in entities)  # Sum up the entities
    
    if count>0:
        return count
    else:
        return 0    

# Update field on specific entity/ row in storage table 
def update_entity_field(table_name, partition_key, row_key, field_name, new_value,field_name2, new_value2,field_name3, new_value3,field_name4, new_value4,field_name5, new_value5):

    try:
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Update the field
        entity[field_name] = new_value
        entity[field_name2] = new_value2
        entity[field_name3] = new_value3
        entity[field_name4] = new_value4
        entity[field_name5] = new_value5

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_entity_field:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")

#Asistant request 
def assistant_request(csv_string, assistant_id, vector_store_id):
    try:
        #openai 
        client  = OpenAI(api_key=openai_key)

        content = csv_string

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
        
        for message in messages.data:
            if message.role == 'assistant':
                for block in message.content:
                    if block.type == 'text':
                        content = block.text.value  # get the text from the block
                        try:
                            logging.debug(f"assistant message: {content}")
                            return content
                        except Exception as e:
                            logging.info(f"error assistant for message step- not message :{e}")
                            return None
    except Exception as e:
     logging.info(f"error assistant - during the process:{e}")
     return None


    #assistant_response = messages.data[-1].content
    #return assistant_response




#get content from storage table 
def get_assistant_details(table_name, partition_key, row_key):
    try:
        logging.info(f"start running get_assistant_details")
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the values of 'assistant_id' and 'vector_store_id' field
        assistant_id = entity.get('assistant_id')
        vector_store_id = entity.get('vector_store_id')
        lableName = entity.get('lableName')
        logging.info(f"get_assistant_details:assistant_id: {assistant_id},vector_store_id: {vector_store_id},ass lableName: {lableName}")
         
        return assistant_id,vector_store_id,lableName
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

#get content csv path from storage table 
def get_content_Csv_path(table_name, partition_key, row_key):
    try:
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the value of 'contentAnalysisCsv' field
        content_csv_path = entity.get('contentCsvConsolidation')
        logging.info(f"content_csv_path: {content_csv_path}")
        return content_csv_path
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
#Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        logging.info("create_servicebus_event:Event created successfully.")
    
    except Exception as e:
        logging.error(f"create_servicebus_event:An error occurred:, {str(e)}")


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
    content_csv_path = get_content_Csv_path(storageTable, caseid, clinicArea)
    content_csv = get_contentcsv_from_storage(content_csv_path)
    logging.info(f"storageTable: {storageTable},caseid: {caseid},clinicArea: {clinicArea}")
    assistant_id, vector_store_id,lableName = get_assistant_details("assistants", clinicArea, "1")
    logging.info(f"main function assistant_id: {assistant_id},vector_store_id: {vector_store_id}")
    if  assistant_id is not None and vector_store_id is not None:
        ass_result = assistant_request(content_csv, assistant_id, vector_store_id)
        if ass_result is None:
            update_entity_field(storageTable, caseid, clinicArea, "assistantResponse", "no response","status",7,"assistantResponsefiltered","no response","assistantResponseNoDisabilities","no response","clinicAreaLableName",lableName)
            updateCaseResult = update_case_generic(caseid,"status",12,"niiMatchingRules",0) #update case status to 12  "NIIMatchingRules faild "
        else:
            filename = f"{clinicArea}.txt"
            #Save Full result
            assistantResponse_path = save_assistantResponse(ass_result,caseid,"assistantResponse",filename)
            #save  records where Disabilities is not  0%
            ass_result_filtered = filter_assistantResponse_v2(ass_result)
            assistantResponsefiltered_path = save_assistantResponse(ass_result_filtered,caseid,"assistantResponse/ass_result_filtered",filename)
            #save  records where Disabilities is  0%
            assistantResponseNoDisabilities = get_assistantResponse_no_Disabilities(ass_result)
            NoDisabilities_path = save_assistantResponse(assistantResponseNoDisabilities,caseid,"assistantResponse/no_disabilities",filename)
            #update record on storage table 
            update_entity_field(storageTable, caseid, clinicArea, "assistantResponse", assistantResponse_path,"status",6,"assistantResponsefiltered",assistantResponsefiltered_path,"assistantResponseNoDisabilities",NoDisabilities_path,"clinicAreaLableName",lableName)
            totalRows = count_rows_in_partition(storageTable,caseid)
            totalTerminationRows = count_rows_status_done(storageTable,caseid)
            #if all clinic areas passed via assistant without errors , update case to done 
            if totalRows==totalTerminationRows: 
                updateCaseResult = update_case_generic(caseid,"status",11,"niiMatchingRules",1) #update case status to 11  "NIIMatchingRules done"
                #preparing data for service bus
                data = { 
                        "caseid" :caseid
                    } 
                json_data = json.dumps(data)
                create_servicebus_event("final-report-process", json_data) #send event to service bus
            logging.info(f"ass_result: {ass_result}")
    else:
        update_entity_field(storageTable, caseid, clinicArea, "assistantResponse", "missing assistant_id or vector_store_id","status",7,"assistantResponsefiltered","missing assistant_id or vector_store_id","assistantResponseNoDisabilities","missing assistant_id or vector_store_id","clinicAreaLableName","missing")
        updateCaseResult = update_case_generic(caseid,"status",12,"niiMatchingRules",0) #update case status to 12  "NIIMatchingRules faild "
        logging.info("Failed to retrieve assistant details.")
    
    