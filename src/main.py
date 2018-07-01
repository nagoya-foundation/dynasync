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
                'AttributeName': 'filepath',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'chunkid',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'filepath',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'chunkid',
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
if 'dynasync_index' not in table_list:
    print("Creating index table.")
    dynamo.create_table(
        TableName = 'dynasync_index',
        AttributeDefinitions = [
            {
                'AttributeName': 'dyna_table',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'filepath',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'dyna_table',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'filepath',
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
dyna_index = dynamo.Table('dynasync_index')
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


