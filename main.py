""" 
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your 
    files.
    
    AUTHOR: Brian Mayer
    
    DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
    requests to DynamoDB. We try to make it as efficient as possible with the 
    help of hashing and modification time to only synchronize new files.
"""

import config
import boto3

# Connect to DynamoDB
print("Connecting...")
dynamo = boto3.client("dynamodb")

if config.configs['table'] not in dynamo.list_tables()["TableNames"]:
    print("Remote table not found, creating one...")

