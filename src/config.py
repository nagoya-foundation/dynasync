# -------------------------------------------------------------------------- #
# Dynasync configuration file. Read or write configs and connect to the given
# DynamoDB table
# -------------------------------------------------------------------------- #

import os
import time
import json
import boto3
import argparse

# ---- Command line arguments ----------------------------------------------- #

print("Dynasync starting " + time.ctime())
parser = argparse.ArgumentParser()
parser.add_argument("--reconf", help="call configure", action="store_true")
args = parser.parse_args()

# ---- Configure ------------------------------------------------------------ #

# Now we create the .config directory if needed
config_dir = os.path.expanduser("~/.config/dynasync")
if not os.path.exists(config_dir):
    os.makedirs(config_dir)
    print("~/.config/dynasync directory created.")

# Check if config file exists in that dir
config_file = config_dir + "/dynasync.conf"
if not os.path.exists(config_file) or args.reconf:
    # Get configuration parameters from user and write to file
    print("Creating configuration file...")
    profile = input("Enter the AWS profile name to use: (default)")
    track_dirs = input("Enter directory to track:")
    collectionName = input("Enter collection name:")

    if track_dirs == "":
        print("You must enter a table name and a directory to track")
        exit(1)

    if profile == "":
        profile = "default"

    with open(config_file, 'w') as file:
        file.write(json.dumps({
            'dir':        track_dirs,
            'collection': collectionName,
            'profile':    profile
            })
        )

with open(config_file, 'r') as file:
    __configs = json.load(file)
    track_dirs = __configs['dir']
    collectionName = __configs['collection']
    profile = __configs.get('profile', 'default')

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
        }
    )
    print("tables created")
