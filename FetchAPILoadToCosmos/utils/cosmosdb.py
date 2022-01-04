from azure.cosmos import CosmosClient, PartitionKey, exceptions
import datetime
from azure.cosmos.container import ContainerProxy
from . import env_config as config
import json

HOST = config.settings['host']
MASTER_KEY = config.settings['master_key']
DATABASE_ID = config.settings['database_id']
CONTAINER_ID = config.settings['container_id']

def read_items(container='last_refresh_time', max_item_count=1) -> list:
    url = HOST
    key = MASTER_KEY
    client = CosmosClient(url, credential=key)
    database_name = DATABASE_ID
    database = client.get_database_client(database_name)
    container_name = container
    container = database.get_container_client(container_name)

    item_list = list(container.read_all_items(max_item_count=max_item_count))
    items = []
    for doc in item_list:
        items.append(doc)
    return items

def load_to_container(data: json, container_name='account', partKey='/account_no') -> None:

    def get_container_create_if_not_exist(name=container_name, partKey=partKey) -> ContainerProxy:
        url = HOST
        key = MASTER_KEY
        client = CosmosClient(url, credential=key)
        database_name = DATABASE_ID
        database = client.get_database_client(database_name)
        container_name = name

        try:
            container = database.create_container(id=container_name, partition_key=PartitionKey(path=partKey))
        except exceptions.CosmosResourceExistsError:
            container = database.get_container_client(container_name)
        except exceptions.CosmosHttpResponseError:
            raise
        return container

    def insert_data(data: json, container: ContainerProxy) -> None:
            container.upsert_item(data)
    
    #data = json.loads(data)
    if data:
        data = json.loads(data)
        #Create container if not exist
        container = get_container_create_if_not_exist(container_name, partKey)
        #Load data to container
        for i in data:
            i['id'] = str(i['id']) # ID must be string type
            i['_load_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_data(i, container)
    else:
        pass
