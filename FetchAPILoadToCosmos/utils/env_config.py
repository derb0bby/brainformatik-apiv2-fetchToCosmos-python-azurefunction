import os

settings = {
    'host': os.environ.get('ACCOUNT_HOST', 'https://brainformatik-crmplus-data.documents.azure.com:443/'),
    'master_key': os.environ.get('ACCOUNT_KEY', 'qDh5aT038fYQzxIvdr9hix2Tt865rFpI3wrZMiMN4jvPzGqfHgT5TcGVt9rzYWBV0kl2176Ble5zaJiO99HMYg=='),
    'database_id': os.environ.get('COSMOS_DATABASE', 'crmplus'),
    'container_id': os.environ.get('COSMOS_CONTAINER', 'Items'),
}