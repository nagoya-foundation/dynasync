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
"""dynasync: an utility to sync files with a remote object file storage
Options: 
  list	displays all remote files
  get <filename>	fetches and prints the contents
  send <filename>	saves the file on remote
dynasync 1.0.0 - Nagoya Foundation, blmayer"""


import boto3
import math
import os
import sys
import lzma
from io import BytesIO
from tqdm import tqdm
from hashlib import md5


chunks = None
files = None


def connect():
    global chunks, files

    storage = boto3.Session(region_name="fr-par").resource(
        's3',
        endpoint_url='https://s3.fr-par.scw.cloud',
        aws_access_key_id=os.getenv('DYNA_KEY'),
        aws_secret_access_key=os.getenv('DYNA_SECRET')
    )
    chunks = storage.Bucket('brein-chunks')
    files = storage.Bucket('brein-files')


# Send a file to remote table
def send_file(file_path):
    file_name = os.path.basename(file_path)

    # Check if file already exists
    content = BytesIO()
    try:
        files.download_fileobj(Key=file_name, Fileobj=content)
    except Exception as e:
        pass

    if len(content.getvalue()) != 0 :
        c = input(f"file {file_name} already exists, overwrite? (y/N)\n")
        if c != 'y':
            return

    # Open file and compress its contents
    with open(os.path.abspath(file_name), 'rb') as file_con:
        file_bytes = file_con.read()

    # Send content in pieces of 512 Kilobytes
    compressed_data = lzma.compress(file_bytes)
    parts = math.ceil(len(compressed_data)/(512*1024))
    hashes = []

    for ck in tqdm(range(parts), ascii=True, desc=file_path):
        # Send the chunk and its md5sum
        chunk = compressed_data[ck*(512*1024):(ck + 1)*(512*1024)]
        hash = str(md5(chunk).hexdigest())
        hashes.append(hash)

        chunks.put_object(Key=hash, Body=chunk)

    files.put_object(
        Key=file_name,
        Body=bytes('\n'.join(hashes), 'utf-8')
    )


def get_file(file_name):
    content = BytesIO()
    files.download_fileobj(Key=file_name, Fileobj=content)

    # Get each chunk
    for c in content.getvalue().split(b'\n'):
        chunk = BytesIO()
        chunks.download_fileobj(Key=c.decode('ascii'), Fileobj=chunk)

        # Collect chunk's contents
        os.write(1, lzma.decompress(chunk.getvalue()))

    print("")


def list_remote_files():
    for fi in files.objects.all():
        print(fi.key)


# Main execution
if len(sys.argv) == 1:
    print(__doc__)
    sys.exit(1)

connect()
if sys.argv[1] == 'list':
    list_remote_files()
elif sys.argv[1] == 'get':
    get_file(sys.argv[2])
elif sys.argv[1] == 'send':
    send_file(sys.argv[2])
