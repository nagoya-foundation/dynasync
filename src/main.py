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

import time
import boto3
import math
import os
import hashlib
import lzma
import tqdm
import config
from decimal import *

# ==== Connect to DynamoDB ====================================================

print("Connecting to AWS...")
session = boto3.Session(profile_name=config.profile)
dynamo = session.resource("dynamodb")

# Check remote table existence
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
    time.sleep(5)

# Now check index table existence
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
    time.sleep(5)

# Table resources
index = dynamo.Table('dynasyncIndex')
table = dynamo.Table('dynasync')

# ==== Function definitions ============================+++++==================

# Send a file to remote table
def sendFile(root, file):
    # Expand ~ in file path file
    fpath = os.path.join(root, file)

    # Verify file size
    # Each item in the index table has about 124 bytes, considering 0 chunks,
    # each chunk hash adds 32 bytes, the item size limit is 400KB set by AWS,
    # so 124 + 32x = 400*1000. Solving the equation gives 12496.125 chunks,
    # as each chunk contains the maximum of 399,901 bytes of the file, so we
    # multiply, and the file size is 4.997.212.883,625 bytes, roughly 4.99GB.
    if os.path.getsize(fpath) > 4997212883:
        print("File " + file + " is too large (> 4,99 GB), skipping...")
        return 0

    # Zero length files may corrupt good files
    if os.path.getsize(fpath) == 0:
        print(fpath + " has 0 length and will not be sent.")
        return

    # Get modification time for updateIndex
    mtime = os.path.getmtime(fpath)

    # Open file and compress its contents
    with open(fpath, 'rb') as file_con:
        fileBytes = file_con.read()

    # Send content in pieces of 399.901 bytes
    chunks = math.ceil(len(fileBytes)/399901)
    hashes = []
    ck = 0
    for ck in tqdm.trange(chunks, ascii=True, desc=os.path.basename(file)):
        # Start the clock
        start = time.time()

        # Send the chunk and its SHA256
        chunk = fileBytes[ck*399901:(ck + 1)*399901]
        hash = hashlib.md5(chunk).hexdigest()
        hashes.append(hash)
        ck += 1

        # Try to send the chunk
        try:
            new_item = table.put_item(
                Item = {
                    'chunkid': hash,
                    'content': lzma.compress(chunk)
                },
                ConditionExpression = 'attribute_not_exists(chunkid)'
            )

            # Wait a sec to preserve throughput
            if len(chunk)/(4000*24*(time.time() - start)) - 1 > 0:
                time.sleep(len(chunk)/(4000*24*(time.time() - start)) - 1)

        except KeyboardInterrupt:
            exit()

        except:
            pass

    # Update index regardless of the size
    updateIndex(file, hashes, mtime)

def updateIndex(file, chunkList, mtime):
    # Start the clock
    start = time.time()

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
    consu = newIndex['ConsumedCapacity']['CapacityUnits'] - time.time() + start

    if consu > 0:
        time.sleep(consu)

def setDeleted(file, mtime):
    print("File " + file + " deleted, updating.")
    index.put_item(
        Item = {
            'filePath': file,
            'mtime':    Decimal(mtime),
            'deleted':  True
        }
    )

def getFile(root, file, chunks):

    # Initialize empty file contents variable
    content = b''

    # Get each chunk
    for i in tqdm.tqdm(range(len(chunks)), ascii=True, desc=os.path.basename(file)):
        start = time.time()
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
        consumed = new_file['ConsumedCapacity']['CapacityUnits']/20 \
            - time.time() + start

        if consumed > 0:
            time.sleep(consumed)

    # Zero length files may corrupt good files
    if len(content) == 0:
        print(file + " has zero length and will not be saved.")
        return

    # Write file to disk
    os.makedirs(os.path.dirname(os.path.join(root, file)), exist_ok=True)
    with open(os.path.join(root, file), 'wb') as file_df:
        try:
            file_df.write(content)
        except IOError as error:
            print("Error writing file:", error)
            abort()
        finally:
            file_df.close()

# Get all files under the selected dir
def collectFiles(root_dir, dir, files):
    # List files in current directory
    dir_files = os.listdir(dir)

    # Search through all files
    # TODO: append modification time information
    for filename in dir_files:
        filepath = os.path.join(dir, filename)

        # Check if it's a normal file or directory
        if os.path.isfile(filepath):
            path = os.path.relpath(filepath, root_dir)
            files.append(path)
        else:
            # We got a directory, enter it for further processing
            collectFiles(root_dir, filepath, files)

# Function to initialize local indexes
def init(track_dirs):
    paths = os.path.expanduser(track_dirs)

    # Put all files in a list
    print("Collecting files under " + track_dirs + "...")
    local_files = []
    collectFiles(paths, paths, local_files)
    print(str(len(local_files)) + " files found.")

    # Get remote tracked files
    print("Querying files in remote table.")
    scanResult = index.scan(
        ExpressionAttributeNames = {
            '#fp': 'filePath',
            '#cl': 'chunks',
            '#mt': 'mtime'
        },
        ExpressionAttributeValues = {
            ':a': False
        },
        ProjectionExpression = '#fp, #mt, #cl',
        FilterExpression = 'deleted = :a',
        ReturnConsumedCapacity = 'TOTAL'
    )
    table_files = scanResult['Items']
    file_count = scanResult['Count']

    # Keep scanning until all results are received
    while 'LastEvaluatedKey' in scanResult.keys():
        # Wait if needed
        if scanResult['ConsumedCapacity']['CapacityUnits'] > 5:
            time.sleep(scanResult['ConsumedCapacity']['CapacityUnits']/5)

        # Scan from the last result
        scanResult = index.scan(
            ExpressionAttributeNames = {
                '#fp': 'filePath',
                '#cl': 'chunks',
                '#mt': 'mtime'
            },
            ExpressionAttributeValues = {
                ':a': False
            },
            ProjectionExpression = '#fp, #mt, #cl',
            FilterExpression = 'deleted = :a',
            ExclusiveStartKey = scanResult['LastEvaluatedKey'],
            ReturnConsumedCapacity = 'TOTAL'
        )

        # Merge all items
        table_files.extend(scanResult['Items'])
        filecount += scanResult['Count']

    # Reformat into a dictionary
    print(str(file_count) + " items read.")
    remote_files = []
    rems = {}
    for file in range(file_count):
        remote_files.append(table_files[file]['filePath'])
        rems[table_files[file]['filePath']] = {
            'mtime': table_files[file]['mtime'],
            'chunks': table_files[file]['chunks']
        }

    # TODO: compare modified times between local and remote files

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        getFile(paths, file, rems[file]['chunks'])

    print("Got all remote files.")

    # Insert modified files in the remote table
    for file in local_files:
        f = os.path.join(paths, file)
        if file not in remote_files or os.path.getmtime(f)>rems[file]['mtime']:
            sendFile(paths, file)

    # Synchronized, now we watch for changes


def resolveDiff(root, olds, news, modTime):
    # Send modified files
    for file in news:
        if os.path.getmtime(os.path.join(root, file)) > modTime:
            sendFile(root, file)

    # Update deleted files
    for file in set(olds) - set(news):
        setDeleted(file, modTime)

    # Send new files
    for file in set(news) - set(olds):
        sendFile(root, file)

# Keep track of files
def watch(track_dirs):

    # Recollect files
    olds = []
    paths = os.path.expanduser(track_dirs)
    collectFiles(paths, paths, olds)

    # Loop forever
    while True:
        # Each round lasts a minimum of 10 seconds
        roundStart = time.time()

        # Get the biggest mod time
        modTimes = [os.path.getmtime(os.path.join(paths, x)) for x in olds]
        if len(modTimes) > 0:
            maxModTime = max(modTimes)
        else:
            maxModTime = 0

        # Recollect files
        news = []
        collectFiles(paths, paths, news)

        # Resolve and send updates
        resolveDiff(paths, olds, news, maxModTime)
        olds = news

        # Wait if necessary
        if time.time() - roundStart < 10:
            time.sleep(10 - time.time() + roundStart)

# ==== Program execution ======================================================

if __name__ == '__main__':
    try:
        # Run initialization
        init(config.track_dirs)

        # Keep walking
        print("Synchronized, watching for changes...")
        watch(config.track_dirs)
    except KeyboardInterrupt:
        print('Interrupted by user. Exiting program...')
        exit(0)


