#! /usr/bin/env python3

# --------------------------------------------------------------------------- #
#  PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
#  files.
#
#  AUTHOR: Brian Mayer
#
#  DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
#  requests to DynamoDB. We try to make it as efficient as possible with the
#  help of hashing and modification time to only synchronize new files.
# 
# Copyright Nagoya Foundation 
# --------------------------------------------------------------------------- #

import time
import boto3
import config
import sync

# ---- Connect to DynamoDB -------------------------------------------------- #

print("Connecting...")
dynamo = boto3.resource("dynamodb")

# Check remote table existence
table_list = [table.name for table in dynamo.tables.all()]
if config.dyna_table not in table_list:
    print("Remote table not found, creating one...")
    dynamo.create_table(
        TableName = config.dyna_table,
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
if config.dyna_index not in table_list:
    print("Creating index table.")
    dynamo.create_table(
        TableName = config.dyna_index,
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

# Table resources
dyna_index = dynamo.Table(config.dyna_index)
dyna_table = dynamo.Table(config.dyna_table)

# ---- Program execution ---------------------------------------------------- #

# Run initialization
sync.init(dyna_table, dyna_index, config.track_dirs)

# Keep walking
print("Synchronized, watching for changes...") 
try:
    sync.watch(dyna_table, dyna_index, config.track_dirs)
except KeyboardInterrupt:
    print('Interrupted')
    exit(0)


# TODO: Use dir as parameter for init and watch

# TODO: Update deleted files

