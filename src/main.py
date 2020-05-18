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
from io import BytesIO
import multiprocessing as mp
from tqdm import tqdm
import config
from hashlib import md5
from boto3.dynamodb.conditions import Key, Attr


# Send a file to remote table
def send_file(file_name):
    # Open file and compress its contents
    with open(os.path.abspath(file_name), 'rb') as file_con:
        file_bytes = file_con.read()
        file_len = len(file_bytes)

    # Send content in pieces of 512 Kilobytes
    chunks = math.ceil(file_len/(512*1024))
    hashes = ""
    ck = 0
    file_path = os.path.relpath(file_name, os.getenv('HOME'))

    # Parallel pool
    pool = mp.Pool(8*mp.cpu_count())
    for ck in tqdm(range(chunks), ascii=True, desc=file_path):
        # Send the chunk and its md5sum
        chunk = file_bytes[ck*(512*1024):(ck + 1)*(512*1024)]
        hash = md5(chunk).hexdigest()
        hashes += hash + "\n"
        ck += 1

        # Try to send the chunk
        data = lzma.compress(chunk)
        pool.apply_async(
            config.storage.put_object,
            args=('brein', hash, BytesIO(data), len(data))
        )

    pool.close()
    pool.join()
    config.storage.put_object(
        'brein-meta',
        file_path,
        BytesIO(hashes),
        len(hashes),
        metadata={
            'deleted': False,
            'size': file_len,
            'name': file_name,
            'm_time': os.path.getmtime(file_path)
        }
    )
    except Exception as e:
        print(e)


def set_deleted(file, mtime):
    # TODO: use try-catch
    config.index.put_item(
        Item={
            'filePath': file,
            'mtime':    int(mtime),
            'deleted':  True
        }
    )


def get_file(file_name):
    file_info = config.storage.get_object('brein-meta', file_name)
    if file_info.status != 200:
        print(file_info.data, file=sys.stderr)

    chunks = file_info.data.split('\n')
    pool = mp.Pool(8*mp.cpu_count())

    # Get each chunk
    contents = pool.map(
        lambda x: config.storage.get_object('brein', x).data,
        chunks,
    )

    # Collect chunk's contents
    os.write(1, ''.join(map(lzma.decompress, contents))+'\n')


def sync_file(file_name):
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
        cons = new_file['ConsumedCapacity']['CapacityUnits'] / \
            20 - time() + start

        if cons > 0:
            sleep(cons)


# TODO: Implement modification time filter as parameter
def list_remote_files():
    query = {
        "ExpressionAttributeNames": {
            '#fp': 'filePath',
            '#cl': 'fileSize',
            '#mt': 'mtime'
        },
        "ExpressionAttributeValues": {':a': 'false'},
        "ProjectionExpression": '#fp, #mt, #cl',
        "FilterExpression": 'deleted = :a'
    }

def _print_file(fi):
    o = config.storage.stat_object('brein-meta', fi.object_name)
    name = o.metadata['x-amz-meta-name']
    size = o.metadata['x-amz-meta-size']
    m_time = o.metadata['x-amz-meta-m_time']
    print(f'{size}\t{m_time}\t{name}')


def list_remote_files():
    files = config.storage.list_objects('brein-meta')

    # Parallel printing
    pool = mp.Pool(mp.cpu_count())
    pool.map(_print_file, files)


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
elif sys.argv[1] == 'send':
    send_file(sys.argv[2])
