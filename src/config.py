# -------------------------------------------------------------------------- #
# Dynasync configuration file. Read or write configs and connect to the given
# DynamoDB table
# -------------------------------------------------------------------------- #

import os
from sys import exit
import time
import json
import boto3


_config_file = os.path.expanduser("~/.config/dynasync.conf")
_dynamo = None
_profile = ""

index = None
table = None


# ---- Configure ------------------------------------------------------------ #


def load():
    global index, table

    # Check if config file exists in that dir
    if not os.path.exists(_config_file):
        print("config file not found in " + _config_file)
        print("run init to configure")
        exit(1)

    with open(_config_file, 'r') as config_file:
        _profile = json.load(config_file)['profile']

    session = boto3.Session(profile_name=_profile)
    _dynamo = session.resource("dynamodb")

    # Table resources
    index = _dynamo.Table('dynasyncIndex')
    table = _dynamo.Table('dynasync')

    table_list = [t.name for t in _dynamo.tables.all()]
    if 'dynasync' not in table_list or 'dynasyncIndex' not in table_list:
        print("dynasync tables not found, run create_tables first")
        exit(2)


def make():
    # Get configuration parameters from user and write to file
    profile = input("Enter the AWS profile name to use: (default)\n")

    if profile == "":
        profile = "default"

    # Now we create the .config directory if needed
    config_dir = os.path.expanduser("~/.config")
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)

    with open(_config_file, 'w') as file:
        file.write('{"profile": "' + profile + '"}')


def create_tables():
    _dynamo.create_table(
        TableName='dynasync',
        AttributeDefinitions=[
            {
                'AttributeName': 'chunkid',
                'AttributeType': 'S'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'chunkid',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 20,
            'WriteCapacityUnits': 24
        },
        SSESpecification={
            'Enabled': True
        }
    )
    time.sleep(5)

    _dynamo.create_table(
        TableName='dynasyncIndex',
        AttributeDefinitions=[
            {
                'AttributeName': 'filePath',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'mtime',
                'AttributeType': 'N'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'filePath',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'mtime',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 1
        },
        SSESpecification={
            'Enabled': True
        },
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'meta-index',
                'KeySchema': [
                    {
                        'AttributeName': 'deleted',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'filePath',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'INCLUDE',
                    'NonKeyAttributes': [
                        'filePath', 'deleted', 'mtime', 'fileSize'
                    ]
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 3,
                    'WriteCapacityUnits': 2
                }
            }
        ]
    )
    print("tables created")
