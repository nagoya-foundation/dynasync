#! /usr/bin/env python3

"""
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
    files.

    AUTHOR: Brian Mayer

    DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
    requests to DynamoDB. We try to make it as efficient as possible with the
    help of hashing and modification time to only synchronize new files.
"""

import hashlib
import lzma
import boto3
import time
import os
import json
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
if os.path.exists(config_file) and not args.reconf:
    print("Config file already exists.")
    with open(config_file, 'r') as file:
        configs = json.load(file)
        watch_dir = configs['dir']
        table_name = configs['table']
else:
    # Get configuration parameters from user and write to file
    watch_dir = os.path.expanduser(input("Enter directory to track:"))
    table_name = input("Enter DynamoDB table name:")
    with open(config_file, 'w') as file:
        file.write(json.dumps({'dir': watch_dir, 'table': table_name}))

print("Configuration finished!\nChecking if it is valid.")
if not os.path.exists(watch_dir):
    print("Entered directory does not exists! Program will fail.")
    exit(-1)

# Connect to DynamoDB
print("Connecting...")
dynamo = boto3.client("dynamodb")

# Check remote table existence
if table_name not in dynamo.list_tables()["TableNames"]:
    print("Remote table not found, creating one...")
    res = dynamo.create_table(
        TableName = table_name,
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

# ---- File tracking -------------------------------------------------------- #

def send_file(file):
    print("Uploading file " + file)
    with open(file, 'rb') as file_con:
        fileBytes = file_con.read()
        content = lzma.compress(fileBytes).decode(errors='surrogateescape')
        if len(content) > 399900:
            print("File too large (> 400 KiB) for DynamoDB, skipping...")
        else: 
            dynamo.put_item(
                TableName = table_name,
                Item = {
                    'filePath': {'S': file},
                    'mtime': {'S': str(os.path.getmtime(file))},
                    'hash': {'S': hashlib.md5(fileBytes).hexdigest()},
                    'size': {'N': str(os.path.getsize(file))},
                    'content': {'S': content},
                    'chunked': {'B': "False"},
                    'exists': {'B': "True"}
                }
            )

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

# Function to initialize local indexes
def init():
    # Put all files in a list
    print("Collecting files under " + watch_dir + "...")
    local_files = []
    remote_files = []
    rems = {}
    collect_files(watch_dir, local_files)

    # Get remote tracked files
    print("Querying files in remote table.")
    table_files = dynamo.scan(
        TableName = table_name,
        ExpressionAttributeNames = {
            '#fp': 'filePath',
            '#mt': 'mtime',
            '#del': 'exists'
        },
        FilterExpression = '#del = :a',
        ExpressionAttributeValues = {
            ':a': {'B': 'True'}
        },
        ProjectionExpression = '#fp, #mt'
    )['Items']

    # Reformat into a dictionary
    for file in range(len(table_files)):
        remote_files.append(table_files[file]['filePath']['S'])
        rems[table_files[file]['filePath']['S']] = {
            'mtime': float(table_files[file]['mtime']['S'])
        }

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        print("Downloading remote file " + file)
        new_file = dynamo.get_item(
            TableName = table_name,
            Key = {
                'filePath': {'S': file},
                'mtime': {'S': str(rems[file]['mtime'])}
            }
        )
        with open(file, 'wb') as file_df:
            file_df.write(new_file['Item']['content']['S'].encode(errors='surrogateescape'))
            file.close()

    # Insert modified files in the remote table
    for file in local_files:
        if file not in remote_files or os.path.getmtime(file) > rems[file]['mtime']:
            send_file(file)

def watch():

    # Recollect files
    tracked_files = []
    collect_files(watch_dir, tracked_files)

    # Loop forever
    while True:
        # Get the bigger mod time
        max_mod = max([os.path.getmtime(x) for x in tracked_files]) 

        # Wait some time
        time.sleep(20)

        # Recollect files
        tracked_files = []
        collect_files(watch_dir, tracked_files)

        # Check if files exists in local index
        for file in tracked_files:
            if os.path.getmtime(file) > max_mod:
                print("File " + file + " modified, sending.")
                send_file(file)

# ---- Program execution ---------------------------------------------------- #

init()
try:
    watch()
except KeyboardInterrupt:
    print('Interrupted')
    exit(0)


# TODO: Use dir as parameter for init and watch

# TODO: Update deleted files

