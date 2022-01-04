import requests
import json
import pandas as pd
import math
import time
import io
import sys
import logging
import datetime
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import *
from .utils.cosmosdb import load_to_container, read_items
import azure.functions as func

# Global settings
AZURE_CONTAINER_NAME = "raw/crm/api"
RECORD_LIST = [] 
MAX_CONCURRENT_THREADS = 30

FILTER_PARAM1 = 'filter[modifiedtime]'
FILTER_PARAM2 = 'filter[folderid]'

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
# Custom Functions


def get_latest_refresh():
    items = read_items(container='last_refresh_time', max_item_count=1)
    items = items[0]
    return str(items['datetime'])

def update_refresh_date(datetime_obj) -> None:
    format = "%Y-%m-%dT%H:%M:%S%z+00:00"
    currentdate = datetime_obj.strftime(format)
    payload = [{
                "id": 1,
                "datetime": currentdate
            }]
    load_to_container(json.dumps(payload), 'last_refresh_time', '/id') 

def upload_to_azureblobstorage(file_content_as_dictionary, source_file_name, output_type="json", containerName=AZURE_CONTAINER_NAME) -> None: 

    AzureBLOBGconnectionString = "DefaultEndpointsProtocol=https;AccountName=atrainlakeprodeu;AccountKey=XARMjD/WGyRgzqoVg4/Fr765BgcnhatONLwQmZHjxVCy239Pe0WAIkmxEHrupggE+VSAkDhBa5u62rTnQVKS0g==;EndpointSuffix=core.windows.net"
    AzureContainerName = containerName

    file_name = f"{str(source_file_name)}.{output_type}"

    #build dataframe from input
    output = io.StringIO()
    data = file_content_as_dictionary
    
    # check which output way to choose
    if output_type == "csv":
        header = file_content_as_dictionary.keys()
        df = pd.DataFrame (data , columns = header)
        output = df.to_csv (index_label="idx", encoding = "utf-8", sep = ";", quoting = 1)

        #upload file
        blob_block = ContainerClient.from_connection_string(
            conn_str = AzureBLOBGconnectionString,
            container_name = AzureContainerName
            )
        blob_block.upload_blob(file_name, output, overwrite=True, encoding='utf-8')

        logging.info(f"File: {file_name} successfully upload to Azure Blob Storage")

    elif output_type == "json":
        json_object = json.dumps(data, indent = 4) 
        output = json_object

        #upload file
        blob_block = ContainerClient.from_connection_string(
            conn_str = AzureBLOBGconnectionString,
            container_name = AzureContainerName
            )
        blob_block.upload_blob(file_name, output, overwrite=True, encoding='utf-8')

        logging.info(f"File: {file_name} successfully upload to Azure Blob Storage")

    else:
        json_object = json.dumps(data, indent = 4) 
        output = json_object

        #upload file
        blob_block = ContainerClient.from_connection_string(
            conn_str = AzureBLOBGconnectionString,
            container_name = AzureContainerName
            )
        blob_block.upload_blob(file_name, output, overwrite=True, encoding='utf-8')

        logging.info(f"File: {file_name} successfully upload to Azure Blob Storage")

def login(session):
    """
    Login to service.
    All other services need a valid authentication, which you will get by calling this action.
    Function returns the accessToken as string.

    API Response:
    {"status": 200,"message": "OK","data": {"accessToken": "eyJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.K1lVDx...","refreshToken": "c7701a80c1eda9761484f0d64f458d7bc2..."}}
    """
    url = "https://atrain-scitrain.brain-app.com/api/auth"

    payload = json.dumps({
    "user": "Kevin_Young",
    "password": "92082353"
    })
    headers = {
    'Accept': 'application/vnd.brainformatik.crm-v1+json',
    'Content-Type': 'application/json'
    }

    response = session.request("POST", url, headers=headers, data=payload)
    jsonData = response.json()

    if response.status_code == 200:
        logging.info("Connection successful established... Return Code: " + str(response.status_code))
        return jsonData["data"]["accessToken"]
    else:
        logging.error(response.status_code)
        sys.exit(0)

def logout(session, token) -> None:
    """ Terminates CRM loginsession """

    url = "https://atrain-scitrain.brain-app.com/api/auth"

    payload={}
    headers = {
    'Authorization': 'Bearer {token}'
    }

    response = session.request("DELETE", url, headers=headers, data=payload)

def currentUser(session, token):
    """
    Retrieve information about the current user.
    Use this action to get details about the currently logged in user.
    
    Example output:
    {"status": 200,"message": "OK", "data": { "id": 1, "someField": "someValue", "anotherField": 1, "imagename": { "name": "string", "content": "string"...}
    """

    url = "https://atrain-scitrain.brain-app.com/api/users/current"

    payload={}
    headers = {
    'Authorization': f'Bearer {token}'
            }

    response = session.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        jsonData = response.json()
        return jsonData["data"]
    else:
        logging.warning(response.status_code)

def getallEntities(session, token):
    """
    Retrieve metadata about all entities.
    Contains detailed information about the entities like translation or permissions.
    Also lists helper entities that are required to create certain entity records, like Currencies for Invoices.
    
    Example output:
    {"status": 200, "message": "OK", "data": [{"id": 23,"name": "Invoice","icon": "faCredit","isEntity": true....
    """

    url = f"https://atrain-scitrain.brain-app.com/api/metadata/entities"
    payload={}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = session.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        jsonData = response.json()
        return jsonData["data"]
    else:
        logging.warning(response.status_code)

def getallEntityDetails(session, token):
    """
    WIP
    """
    all_entities = getallEntities(session=session, token=token)
    entitiy_ids = [i['id'] for i in all_entities]
    record_list = []

    for id in entitiy_ids:
        url = f"https://atrain-scitrain.brain-app.com/api/metadata/entities/{id}"
        payload={}
        headers = {'Authorization': f'Bearer {token}'}
        with session.request("GET", url, headers=headers, data=payload) as response:
            resp_json = response.json()
            if response.status_code == 200:
                content = resp_json["data"]
                content['id'] = str(id)
                record_list.append(content) # Write to mutuable object as an workaround, since threads can not return anything.
            else:
                logging.warning("record id: " + str(id) + " code: " + str(response.status_code))
    return record_list

def getallRecordIDs(session, token, entityid, param: dict = None):
    """
    Retrieve records for an entity. Use this action to get a list of records for an entity.
    
    Returns -> A list of all available record 'id's for the given entity

    Have a look at query parameters for details on how to modify the result.
    https://atrain-scitrain.brain-app.com/api/docs#tag/Records/paths/~1entity~1{entityId}~1records/get
    """

    parameters = {}
    parameters['limit'] = 200
    parameters['offset'] = 0
    parameters['filter[modifiedtime]'] = '>' + get_latest_refresh()

    if param:
        parameters = dict(parameters, **param)

    data = []


    url = f"https://atrain-scitrain.brain-app.com/api/entity/{entityid}/records"
    headers = {'Authorization': f'Bearer {token}'}

    response = session.request("GET", url, headers=headers, params=parameters)

    if response.status_code == 200:
        jsonData = response.json()
        total = jsonData["metadata"]["total"]

        if total == 0:
            logging.info(f'Records returned: {total}, skip...')
            return []

        elif total > 0:
            logging.info(f'Records available: {total}')
            pages = math.ceil(total / parameters.get('limit', 200))
            logging.debug("pages: " + str(pages))

            for i in range(pages):
                
                parameters['fields'] = 'id|createdtime|modifiedtime'
                #params = {'limit': limit,'offset': offset, 'fields': 'id|createdtime|modifiedtime', FILTER_PARAM1: FILTER_MODIFIED_DATE} # only query for the id and the createdtime to remove excess data (you need to specifice at least two fields)
                logging.debug('Request parameters: ' + str(parameters) )
                response = session.request("GET", url, headers=headers, params=parameters)
                jsonresponse = response.json()
                for x in jsonresponse["data"]:
                    data.append(x)
                parameters['offset'] += parameters.get('limit', 200)
            
            df = pd.DataFrame(data)
            return df['id'].values.tolist()
        
    else:
        logging.warning(response.status_code)

def getrecordDetail(session, token, entityname, entityid, recordid, get_fields=None):
    """
    Retrieve a specific record of an entity.
    This will return all the data, including line items, of a single record.

    Function returns -> 'data' content as python dictionary.

    API response:
    "status": 200, "message": "OK", "metadata": {....., "data": {"id": 1, "someField": "someValue", "anotherField": 1, "someFile": {}, "lineItems": [...]}}
    """
    url = f"https://atrain-scitrain.brain-app.com/api/entity/{entityid}/records/{recordid}"
    payload={}
    params={}
    if get_fields:
        params = dict(params, **get_fields)
    headers = {'Authorization': f'Bearer {token}'}

    with session.request("GET", url, headers=headers, data=payload, params=params) as response:
        resp_json = response.json()
        if response.status_code == 200:
            RECORD_LIST.append(resp_json["data"]) # Write to mutuable object as an workaround, since threads can not return anything.
        else:
            logging.warning("record id: " + str(recordid) + " code: " + str(response.status_code))

def loadDocumentsToBLOB(session, token, entityname, entityid, recordid, get_fields=None):
    """
    Retrieve a specific record of an entity.
    This will return all the data, including line items, of a single record.

    Function returns -> 'data' content as python dictionary.

    API response:
    "status": 200, "message": "OK", "metadata": {....., "data": {"id": 1, "someField": "someValue", "anotherField": 1, "someFile": {}, "lineItems": [...]}}
    """
    url = f"https://atrain-scitrain.brain-app.com/api/entity/{entityid}/records/{recordid}"
    payload={}
    params={}
    if get_fields:
        params = dict(params, **get_fields)
    headers = {'Authorization': f'Bearer {token}'}

    with session.request("GET", url, headers=headers, data=payload, params=params) as response:
        resp_json = response.json()
        if response.status_code == 200:
            file_name = f'{resp_json["data"]["id"]}_{resp_json["data"]["note_no"]}'
            upload_to_azureblobstorage(resp_json["data"], file_name, containerName="raw/crm/documents")
        else:
            logging.warning("record id: " + str(recordid) + " code: " + str(response.status_code))
                         
def patch_https_connection_pool(**constructor_kwargs):
    """
    This allows to override the default parameters of the
    HTTPConnectionPool constructor.
    For example, to increase the poolsize to fix problems
    with "HttpSConnectionPool is full, discarding connection"
    call this function with maxsize=16 (or whatever size
    you want to give to the connection pool)
    """
    from urllib3 import connectionpool, poolmanager

    class MyHTTPSConnectionPool(connectionpool.HTTPSConnectionPool):
        def __init__(self, *args,**kwargs):
            kwargs.update(constructor_kwargs)
            super(MyHTTPSConnectionPool, self).__init__(*args,**kwargs)
    poolmanager.pool_classes_by_scheme['https'] = MyHTTPSConnectionPool

def main(mytimer: func.TimerRequest) -> None:

    # Overwrite default request pool size and start new session    
    patch_https_connection_pool(maxsize = MAX_CONCURRENT_THREADS)
    session = requests.session()

    # Get fresh login token from CRM
    token = login(session)

    # Declare function which allows for multi threading of target function 'getrecordDetail', 
    def getDetailRecordsFrom(modulename, moduleno, filterfor=None):
        
        list_of_records = getallRecordIDs(session, token, moduleno, filterfor) # get the list of record IDs for the choosen crm module
        if list_of_records:
            threads = []

            logging.info(f"Start fetching {modulename} from api.")

            with ThreadPoolExecutor(max_workers = MAX_CONCURRENT_THREADS) as executor:
                for record in list_of_records:
                    threads.append(executor.submit(getrecordDetail, session=session, token=token, entityname=modulename, entityid=moduleno, recordid=record))
    
    def getDocumentRecordsFrom(modulename, moduleno, filterfor=None):
        list_of_records = getallRecordIDs(session, token, moduleno, filterfor) # get the list of record IDs for the choosen crm module
        if list_of_records:
            threads = []

            logging.info(f"Start fetching {modulename} from api.")

            with ThreadPoolExecutor(max_workers = MAX_CONCURRENT_THREADS) as executor:
                for record in list_of_records:
                    threads.append(executor.submit(loadDocumentsToBLOB, session=session, token=token, entityname=modulename, entityid=moduleno, recordid=record))

    # Start retreive and upload procedure (threded for concurrency) and finally upload to azure blob storage

    process_start = time.perf_counter() # Initialize timer start for performance measurement
    save_refresh_time = datetime.datetime.utcnow()
    logging.info(f"Begin fetching...")

    #Load Metadata to Cosmos
    load_to_container(json.dumps(getallEntityDetails(session, token)), 'entitie_detail', '/id')
    load_to_container(json.dumps(getallEntities(session=session, token=token)), 'entities_desc', '/name')

    #Load Records to Cosmos
    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_potentials_detailrecord",2)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_potentials_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'potentials', '/potential_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_account_detailrecord",6)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_account_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'accounts', '/account_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()    

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_contacts_detailrecord",4)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_contacts_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'contacts', '/contact_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_leads_detailrecord",7)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_leads_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'leads', '/lead_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_products_detailrecord",14)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_products_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'products', '/product_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_quotes_detailrecord",20)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_quotes_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'quotes', '/quote_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()     

    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_salesorder_detailrecord",22)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_salesorder_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'salesorder', '/salesorder_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()
    
    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_invoice_detailrecord",23)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_invoice_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'invoice', '/invoice_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()
    
    exec_start = time.perf_counter()
    getDetailRecordsFrom("crm_api_pmProjects_detailrecord",46)
    #upload_to_azureblobstorage(RECORD_LIST, 'crm_api_pmProjects_detailrecord')
    load_to_container(json.dumps(RECORD_LIST), 'projects', '/pm_project_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")
    RECORD_LIST.clear()
    
    exec_start = time.perf_counter()
    getDocumentRecordsFrom("Documents", 8) # Last update | filterfor={'filter[modifiedtime]': '>2021-12-20T08:04:16+00:00'}
    #load_to_container(json.dumps(RECORD_LIST), 'documents', '/note_no')
    logging.info(f"Time spent: {round(time.perf_counter() - exec_start)} seconds")

    # Update refresh date in CosmosDB
    update_refresh_date(save_refresh_time)
    
    process_finish = time.perf_counter() # Stop timer
    logout(session, token) # End session with CRM
    minutes = round(((process_finish - process_start) / 60), 2)

    logging.info(f"Procedure complete, execution duration: ({minutes} min.)")

    # Standard Azure stuff
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
