#! /usr/bin/env python3

"""
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
    files.

    AUTHOR: Brian Mayer

    DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
    requests to DynamoDB. We try to make it as efficient as possible with the
    help of hashing and modification time to only synchronize new files.
"""

import os
import hashlib
import lzma
import json
import boto3
import argparse
import sync

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
configs = {}
if os.path.exists(config_file) and not args.reconf:
    print("Config file already exists.")
    with open(config_file, 'r') as file:
        configs = json.load(file)
else:
    # Get configuration parameters from user and write to file
    configs['dir'] = os.path.expanduser(input("Enter directory to track:"))
    configs['table'] = input("Enter DynamoDB table name:")
    with open(config_file, 'w') as file:
        file.write(json.dumps(configs))

print("Configuration finished!\nChecking if it is valid.")
if not os.path.exists(configs['dir']):
    print("Entered directory does not exists! Program will fail.")
    exit(-1)

# ---- Connect to DynamoDB -------------------------------------------------- #

print("Connecting...")
dynamo = boto3.client("dynamodb")

# Check remsote table existence
if configs['table'] not in dynamo.list_tables()["TableNames"]:
    print("Remote table not found, creating one...")
    res = dynamo.create_table(
        TableName = configs['table'],
        AttributeDefinitions = [
            {
                'AttributeName': 'filePath',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'mtime',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'filePath',
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
    print("Table created.")
else:
    print("Table found.")

# Get all files under the selected dir
def collect_files(root_dir, files):
    # List files in current directory
    root_dir_files = os.listdir(root_dir)
    # Search through all files
    for filename in root_dir_files:
        filepath = os.path.join(root_dir, filename)
        # Check if it's a normal file or directory
        if os.path.isfile(filepath):
            files.append(filepath)
        else:
            # We got a directory, enter it for further processing
            collect_files(filepath, files)

# ---- File tracking -------------------------------------------------------- #
sync.init()

# Put all files in a list
# TODO: Download files from dynamo

# TODO: Update deleted files

