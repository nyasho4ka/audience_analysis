import json

with open('config.json') as config:
    CONFIG = json.loads(config.read())


def get_phone_and_password():
    credentials = CONFIG['credentials']
    return credentials['phone'], credentials['password']


def get_required_table_fields():
    return CONFIG['csv_processing']['fields']
