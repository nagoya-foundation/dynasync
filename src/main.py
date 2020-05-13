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
import sys
import lzma
from tqdm import tqdm
import config
from time import time, sleep
from hashlib import md5
from boto3.dynamodb.conditions import Key, Attr


# Send a file to remote table
def send_file(file_name):
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
    for ck in tqdm(chunks, ascii=True, desc=file_path):
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
    try:
        config.index.put_item(
            Item={
                'filePath': file_path,
                'fileSize': fileLen,
                'mtime':    int(mtime),
                'deleted':  'false',
                'chunks':   hashes
            }
        )
    except Exception as e:
        print(e)


def set_deleted(file, mtime):
    # TODO: use try-catch
    config.index.put_item(
        Item={
            'filePath': file,
            'mtime':    Decimal(mtime),
            'deleted':  True
        }
    )


def get_file(file_name):
    file_info = config.index.query(
        IndexName='filePath-index',
        KeyConditionExpression=Key('filePath').eq(file_name)
    )
    if len(file_info['Items']) == 0:
        print('file not found', file=sys.stderr)
        return

    chunks = file_info['Items'][0]['chunks']

    # Get each chunk
    for chunk in tqdm(chunks, ascii=True, desc=file_name, file=sys.stderr):
        start = time()
        try:
            new_file = config.table.get_item(
                Key={
                    'chunkid': chunk
                },
                ReturnConsumedCapacity='TOTAL'
            )
            # Collect chunk's contents
            os.write(1, lzma.decompress(new_file['Item']['content'].value))

        except Exception as e:
            print(e, file=sys.stderr)
            return

        # Wait based on consumed capacity
        cons=new_file['ConsumedCapacity']['CapacityUnits'] / \
            20 - time() + start

        if cons > 0:
            sleep(cons)


# TODO: Implement modification time filter as parameter
def list_remote_files():
    query={
        "ExpressionAttributeNames": {
            '#fp': 'filePath',
            '#cl': 'fileSize',
            '#mt': 'mtime'
        },
        "ExpressionAttributeValues": {':a': 'false'},
        "ProjectionExpression": '#fp, #mt, #cl',
        "FilterExpression": 'deleted = :a'
    }

    scan_result=config.index.scan(query, IndexName='meta-index')
    for fi in scan_result['Items']:
        name=fi['filePath']
        size=fi.get('fileSize', 0)
        m_time=int(fi['mtime'])
        print(f'{size}\t{m_time}\t{name}')

    # Keep scanning until all results are received
    while 'LastEvaluatedKey' in scan_result.keys():
        # Scan from the last result
        scan_result=config.index.scan(
            query,
            IndexName='meta-index',
            ExclusiveStartKey=scan_result['LastEvaluatedKey']
        )
        for fi in scan_result['Items']:
            name=fi['filePath']
            size=fi.get('fileSize', 0)
            m_time=fi['mtime']
            print(f'{size}\t{m_time}\t{name}')


# Main execution
if sys.argv[1] == 'init':
    config.make()
    sys.exit(0)
elif sys.argv[1] == 'create_tables':
    config.create_tables()
    sys.exit(0)

config.load()

if sys.argv[1] == 'list':
    list_remote_files()
elif sys.argv[1] == 'get':
    get_file(sys.argv[2])
