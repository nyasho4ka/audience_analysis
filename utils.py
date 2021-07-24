import json
import time
import argparse

with open('config.json') as config:
    CONFIG = json.loads(config.read())


def get_vk_credentials():
    credentials = CONFIG['vk']['credentials']
    return credentials['phone'], credentials['password']


def get_spotify_credentials():
    credentials = CONFIG['spotify']['credentials']
    return credentials['client_id'], credentials['client_secret']


def get_required_table_fields():
    return CONFIG['csv_processing']['fields']


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--group_id', dest='group_id', type=str, required=True,
    )
    return parser.parse_args()


def timer(fn):
    def wrapper():
        start_time = time.time()
        fn()
        print(f"TIME HAS PASSED: {time.time() - start_time} s.")
    return wrapper
