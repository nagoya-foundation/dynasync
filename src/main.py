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
#  Copyright Nagoya Foundation
# --------------------------------------------------------------------------- #

import boto3
import math
import os
import lzma
import tqdm
import config
from time import time, sleep
from hashlib import md5
from decimal import *

# ==== Connect to DynamoDB ====================================================

# TODO: use try-catch
print("Connecting to AWS...")
session = boto3.Session(profile_name=config.profile)
dynamo = session.resource("dynamodb")

# Check remote table existence
# TODO: use try-catch
tableList = [table.name for table in dynamo.tables.all()]
if 'dynasync' not in tableList:
    print("Remote table not found, creating one...")
    dynamo.create_table(
        TableName = 'dynasync',
        AttributeDefinitions = [
            {
                'AttributeName': 'chunkid',
                'AttributeType': 'S'
            }
        ],
        KeySchema = [
            {
                'AttributeName': 'chunkid',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 20,
            'WriteCapacityUnits': 24
        },
        SSESpecification = {
            'Enabled': True
        }
    )
    print("Table created. Wait a little.")
    sleep(5)

# Now check index table existence
# TODO: use try-catch
if 'dynasyncIndex' not in tableList:
    print("Creating index table.")
    dynamo.create_table(
        TableName = 'dynasyncIndex',
        AttributeDefinitions = [
            {
                'AttributeName': 'filePath',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'mtime',
                'AttributeType': 'N'
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
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 1
        },
        SSESpecification = {
            'Enabled': True
        }
    )
    print("Index table created. Wait a little.")
    sleep(5)

# Table resources
index = dynamo.Table('dynasyncIndex')
table = dynamo.Table('dynasync')

# Global variables
track_dirs = os.path.expanduser(config.track_dirs)

# ==== Function definitions ============================+++++==================

# Send a file to remote table
def sendFile(file_name):
    file_path = os.path.abspath(file_name)

    # Open file and compress its contents
    with open(file_path, 'rb') as file_con:
        fileBytes = file_con.read()
        fileLen = len(fileBytes)

    # Verify file size
    # Each item in the index table has about 124 bytes, considering 0 chunks,
    # each chunk hash adds 32 bytes, the item size limit is 400KB set by AWS,
    # so 124 + 32x = 400*1000. Solving the equation gives 12496.125 chunks,
    # as each chunk contains the maximum of 399,901 bytes of the file, so we
    # multiply, and the file size is 4.997.212.883,625 bytes, roughly 4.99GB.
    if fileLen > 4997212883:
        print("File " + file_path + " is too large (> 4,99 GB), skipping...")
        return 0

    # Zero length files may corrupt good files
    if fileLen == 0:
        print(file_path + " has 0 length and will not be sent.")
        return

    # Get modification time for updateIndex
    mtime = os.path.getmtime(file_path)

    # Send content in pieces of 399.901 bytes
    chunks = math.ceil(fileLen/399901)
    hashes = []
    ck = 0
    for ck in trange(chunks, ascii=True, desc=file_path):
        # Start the clock
        start = time()

        # Send the chunk and its md5sum
        chunk = fileBytes[ck*399901:(ck + 1)*399901]
        hash = md5(chunk).hexdigest()
        hashes.append(hash)
        ck += 1

        # Try to send the chunk
        try:
            config.table.put_item(
                Item={
                    'chunkid': hash,
                    'content': lzma.compress(chunk)
                },
                ConditionExpression='attribute_not_exists(chunkid)'
            )

            # Wait a sec to preserve throughput
            if len(chunk)/(4000*24*(time() - start)) - 1 > 0:
                sleep(len(chunk)/(4000*24*(time() - start)) - 1)

        except KeyboardInterrupt:
            exit()

        except:
            pass

    # Update index regardless of the size
    updateIndex(file, hashes, mtime)

def updateIndex(file, chunkList, mtime):
    # Start the clock
    start = time()

    # TODO: use try-catch
    newIndex = index.put_item(
        Item = {
            'filePath': file,
            'mtime':    Decimal(mtime),
            'deleted':  False,
            'chunks':   chunkList
        },
        ReturnConsumedCapacity = 'TOTAL'
    )

    # Wait a sec to preserve throughput
    consu = newIndex['ConsumedCapacity']['CapacityUnits'] - time() + start

    if consu > 0:
        sleep(consu)

def setDeleted(file, mtime):
    print("File " + file + " deleted, updating.")
    # TODO: use try-catch
    index.put_item(
        Item = {
            'filePath': file,
            'mtime':    Decimal(mtime),
            'deleted':  True
        }
    )

def getFile(file, chunks):

    # Expand ~ in file path
    file_path = os.path.join(os.path.expanduser(track_dirs), file)

    # Initialize empty file contents variable
    content = b''

    # Get each chunk
    for i in tqdm.tqdm(range(len(chunks)), ascii=True, desc=os.path.basename(file)):
        start = time()
        try:
            new_file = table.get_item(
                Key = {
                    'chunkid': chunks[i]
                },
                ReturnConsumedCapacity = 'TOTAL'
            )
            # Collect chunk's contents
            content += lzma.decompress(new_file['Item']['content'].value)

        except KeyboardInterrupt:
            exit()

        except:
            print("Chunk " + chunks[i] + " not found, file will not be saved!")
            return

        # Wait based on consumed capacity
        cons = new_file['ConsumedCapacity']['CapacityUnits']/20 - time() + start

        if cons > 0:
            sleep(cons)

    # Write file to disk
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(os.path.join(file_path, file), 'wb') as file_df:
        try:
            file_df.write(content)
        except IOError as error:
            print("Error writing file:", error)
            abort()
        finally:
            file_df.close()

# Get all files under the selected dir
def collectFiles(dir, files):
    # List files in current directory
    dir_files = os.listdir(dir)

    # Search through all files
    for filename in dir_files:
        file_path = os.path.join(dir, filename)

        # Check if it's a normal file or directory
        if os.path.isfile(file_path):
            path = os.path.relpath(file_path, track_dirs)
            files[path] = os.path.getmtime(file_path)
        else:
            # We got a directory, enter it for further processing
            collectFiles(file_path, files)

# TODO: Implement modification time filter as parameter
def getRemoteFiles():

    query = {
        "ExpressionAttributeNames": {
            '#fp': 'filePath', '#cl': 'chunks', '#mt': 'mtime'
        },
        "ExpressionAttributeValues": {':a': False},
        "ProjectionExpression": '#fp, #mt, #cl',
        "FilterExpression": 'deleted = :a',
        "ReturnConsumedCapacity": 'TOTAL'
    }

    print("Querying files in remote table.")
    # TODO: use try-catch
    scanResult = index.scan(query)
    table_files = scanResult['Items']

    # Keep scanning until all results are received
    while 'LastEvaluatedKey' in scanResult.keys():
        # Wait if needed
        if scanResult['ConsumedCapacity']['CapacityUnits'] > 5:
            sleep(scanResult['ConsumedCapacity']['CapacityUnits']/5)

        # Scan from the last result
        # TODO: use try-catch
        scanResult = index.scan(
            query,
            ExclusiveStartKey = scanResult['LastEvaluatedKey']
        )

        # Merge all items
        table_files.extend(scanResult['Items'])

    # Reformat into a dictionary
    remote_files = {}
    for fi in table_files:
        candidate = remote_files.get(fi['filePath'], {'mtime': 0})
        if candidate['mtime'] < fi['mtime']:
            remote_files[fi['filePath']] = {
                'mtime': fi['mtime'],
                'chunks': fi['chunks']
            }


    return remote_files

# Function to initialize local indexes
def sync():

    # Put all files in a list
    local_files = {}
    collectFiles(track_dirs, local_files)

    # Synchronized, now we watch for changes
    while True:
        # Each round lasts a minimum of 20 seconds
        roundStart = time()

        # Recollect files
        # TODO: use try-catch
        remote_files = getRemoteFiles()
        new_local_files = {}
        collectFiles(track_dirs, new_local_files)

        # Resolve and send updates
        for file in new_local_files:
            if new_local_files[file] > max(
                local_files.get(file, 0),
                remote_files.get(file, {'mtime': 0})['mtime']
            ):
                sendFile(file)

        # Update deleted files
        for file in set(local_files) - set(new_local_files):
            setDeleted(file, time())

        # Delete files
        for file in set(remote_files) - set(new_local_files):
            getFile(track_dirs, file, remote_file[file]['chunks'])
            print('deleting file ' + file)
            os.remove(os.path.join(track_dirs, file))

        local_files = new_local_files

        # Wait if necessary
        if time() - roundStart < 20:
            sleep(20 - time() + roundStart)


# ==== Program execution ======================================================

if __name__ == '__main__':
    try:
        # Run initialization
        sync()

# Main execution
if sys.argv[1] == 'init':
    config.make()
    sys.exit(0)
elif sys.argv[1] == 'create_tables':
    config.create_tables()
    sys.exit(0)

config.load()

if sys.argv[1] == 'list':
    listRemoteFiles()
