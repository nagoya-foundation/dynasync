"""
	PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
	files.

	AUTHOR: Brian Mayer

	DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
	requests to DynamoDB. We try to make it as efficient as possible with the
	help of hashing and modification time to only synchronize new files.
"""

import os
import config
import boto3

# ---- Connect to DynamoDB ------------------------------------------------- #

print("Connecting...")
dynamo = boto3.client("dynamodb")

# Check remote table existence
if config.configs['table'] not in dynamo.list_tables()["TableNames"]:
	print("Remote table not found, creating one...")
	res = dynamo.create_table(
		TableName = config.configs['table'],
		AttributeDefinitions = [
			{
				'AttributeName': 'file',
				'AttributeType': 'S'
			},
			{
				'AttributeName': 'latest',
				'AttributeType': 'B'
			}
		],
		KeySchema = [
			{
				'AttributeName': 'latest',
				'KeyType': 'HASH'
			},
			{
				'AttributeName': 'file',
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
tracked_files = []
collect_files(config.configs['dir'], tracked_files)

# Get remote tracked files
remote_files = dynamo.query(
	TableName = config.configs['table'],
	ExpressionAttributeValues = {
		':v1': {
			'B': 'True'
		}
	},
	KeyConditionExpression = 'latest = :v1'
)

# Track all files
for file in tracked_files:
	if os.path.getmtime(file)
