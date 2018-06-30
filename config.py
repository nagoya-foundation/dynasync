# -------------------------------------------------------------------------- #
# Dynasync condfiguration file. Read or write configs and connect to the given
# DynamoDB table
# -------------------------------------------------------------------------- #

import os
import time
import json
import boto3
import argparse

# ---- Command line arguments ----------------------------------------------- #

parser = argparse.ArgumentParser()
parser.add_argument("--reconf", help = "call configure", action = "store_true")
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
    print("Creating configuration file.")
    track_dirs = os.path.expanduser(input("Enter directory to track:"))
    dyna_table = input("Enter DynamoDB table name:")
    with open(config_file, 'w') as file:
        file.write(json.dumps({'dir': track_dirs, 'table': dyna_table}))

with open(config_file, 'r') as file:
    configs = json.load(file)
    track_dirs = configs['dir']
    dyna_table = configs['table']
    dyna_index = dyna_table + "_index"

print("Configuration finished!\nChecking if it is valid.")
if not os.path.exists(track_dirs):
    print("Entered directory does not exists! Program will fail.")
    exit(-1)

# ---- Connect to DynamoDB -------------------------------------------------- #

print("Connecting...")
dynamo = boto3.client("dynamodb")

# Check remote table existence
table_list = dynamo.list_tables()["TableNames"]
if dyna_table not in table_list:
    print("Remote table not found, creating one...")
    dynamo.create_table(
        TableName = dyna_table,
        AttributeDefinitions = [
            {
                'AttributeName': 'deleted',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'mtime',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'deleted',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'mtime',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print("Table created. Wait a little.")
    time.sleep(5)
else:
    print("Table found.")

# Now check index table existence
if dyna_index not in table_list:
    print("Creating index table.")
    dynamo.create_table(
        TableName = dyna_index,
        AttributeDefinitions = [
            {
                'AttributeName': 'dyna_table',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'filePath',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'dyna_table',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'filePath',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    time.sleep(5)

