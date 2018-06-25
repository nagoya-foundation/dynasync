#!/usr/bin/python3

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

# ---- Configure ----------------------------------------------------------- #

# Now we create the .config directory if needed
config_dir = os.path.expanduser("~/.config/dynasync")
if not os.path.exists(config_dir):
    os.makedirs(config_dir)
    print("~/.config/dynasync directory created.")

# Check if config file exists in that dir
config_file = config_dir + "/dynasync.conf"
configs = {}
if os.path.exists(config_file):
    print("Config file already exists.")
    with open(config_file, 'r') as file:
        configs = json.load(file)
        configs['dir'] = os.path.expanduser(configs['dir'])
else:
    # Get configuration parameters from user and write to file
    configs['dir'] = input("Enter directory to track:")
    configs['table'] = input("Enter DynamoDB table name:")
    with open(config_file, 'w') as file:
        file.write(json.dumps(configs))

print("Configuration finished!")

# ---- Connect to DynamoDB ------------------------------------------------- #

print("Connecting...")
dynamo = boto3.client("dynamodb")

# Check remote table existence
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
                'AttributeName': 'modTime',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'filePath',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'modTime',
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

# ---- File tracking ------------------------------------------------------- #

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

# Put all files in a list
print("Collecting files under " + configs['dir'] + " ...")
tracked_files = []
collect_files(configs['dir'], tracked_files)

# Create a dict for each file
local_files = {}

# Append file information like size, mtime &c
print("Creating files attributes...")
for file in tracked_files:
    handle = open(file, 'rb')
    fileBytes = handle.read()
    content = str(lzma.compress(fileBytes))
    local_files[file] = {
        'modTime': os.path.getmtime(file),
        'hash': hashlib.md5(fileBytes).hexdigest(),
        'content': content,
        'size': len(content),
        'deleted': "False"
    }
    handle.close()
    print("File " + file + " processed.")

# Get remote tracked files
print("Querying files in remote table.")
table_files = dynamo.scan(TableName = configs['table'])['Items']

# Reformat into a dictionary
remote_files = {}
for file in range(len(table_files)):
    remote_files[table_files[file]['filePath']['S']] = {
        'modTime': float(table_files[file]['modTime']['S']),
        'hash': table_files[file]['hash']['S'],
        'content': table_files[file]['content']['S'],
        'size': int(table_files[file]['size']['N']),
        'deleted': table_files[file]['deleted']['B']
    }

# Find existing but modified files
common_files = set(local_files).intersection(set(remote_files))
modified_files = {x for x in common_files if local_files[x]['modTime'] > remote_files[x]['modTime']}

if len(modified_files) == 0:
    print("No modified files.")
else:
    print("Modified files to be uploaded:")
    print(modified_files)

# Find new local files
newFiles = local_files.keys() - remote_files.keys()

if len(newFiles) == 0:
    print("No new files.")
else:
    print("New files to be uploaded:")
    print(modified_files)

# Insert them in the remote table
for newFile in modified_files.union(newFiles):
    print("Uploading file " + newFile)
    dynamo.put_item(
        TableName = 'docs', 
        Item = {
            'filePath': {'S': newFile},
            'modTime': {'S': str(local_files[newFile]['modTime'])},
            'hash': {'S': local_files[newFile]['hash']},
            'size': {'N': str(local_files[newFile]['size'])},
            'content': {'S': local_files[newFile]['content']},
            'deleted': {'B': "False"}
        }
    )

# TODO: Update deleted files

