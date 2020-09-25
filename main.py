#! /usr/bin/env python3

# --------------------------------------------------------------------------- #
#  PROJECT: fsync: An utility to save and recover your files on a objetct
#  stogare service.
#
#  AUTHOR: Brian Mayer
#
#  DESCRIPTION: Main file for fsync, it uses the boto3 API to make requests
#  to a object storage. We try to make it as efficient as possible with the
#  help of splitting and compression to store and reutilize chunks..
#
#  Copyright Nagoya Foundation
# --------------------------------------------------------------------------- #

"""fsync: an utility to sync files with a remote object file storage
Usage:
  fsync command [options] [filename]
Available commands are: 
  list	                displays all remote files
  get   <filename>	fetches and prints the contents
  send  <filename>	saves the file on remote
  configure             run configuration steps
Available options for send:
  -y    yes to overwrite file
  -n    <name> send filename as name
fsync 1.0.0 - Nagoya Foundation, blmayer"""


import boto3
import math
import json
import os
import sys
import lzma
from io import BytesIO
from tqdm import tqdm
from hashlib import md5


chunks = None
files = None

configfile = os.path.expanduser("~/.config/fsync.conf")
config = {}

def configure():
    global config

    if os.path.exists(configfile):
        config = json.loads(open(configfile).read())
    else:
        print("No configuration file found, running configure")
        config["url"] = input("Enter the storage URL:\n")
        config["region"] = input("Enter the region:\n")
        config["key"] = input("Enter the key ID:\n")
        config["secret"] = input("Enter the secret:\n")
    
        json.dump(config, open(configfile, "w"))

        storage = boto3.Session(region_name=config["region"]).resource(
            's3',
            endpoint_url=config["url"],  # 'https://s3.fr-par.scw.cloud',
            aws_access_key_id=config["key"],  # os.getenv('DYNA_KEY'),
            aws_secret_access_key=config["secret"]  # os.getenv('DYNA_SECRET')
        )
        storage.create_bucket(Bucket="fsync-chunks")
        storage.create_bucket(Bucket="fsync-files")


def connect():
    global chunks, files

    storage = boto3.Session(region_name=config["region"]).resource(
        's3',
        endpoint_url=config["url"],  ## 'https://s3.fr-par.scw.cloud',
        aws_access_key_id=config["key"],  # os.getenv('DYNA_KEY'),
        aws_secret_access_key=config["secret"]  # os.getenv('DYNA_SECRET')
    )
    chunks = storage.Bucket('fsync-chunks')
    files = storage.Bucket('fsync-files')


# Send a file to remote table
def send_file(file_path, name, y):
    file_name = os.path.basename(file_path)
    if name is not None:
        file_name = name

    # Check if file already exists
    if not y:
        content = BytesIO()
        try:
            files.download_fileobj(Key=file_name, Fileobj=content)
        except:
            pass

        if len(content.getvalue()) != 0:
            c = input(f"file {file_name} already exists, overwrite? (y/N)\n")
            if c != 'y':
                return

    # Open file and compress its contents
    with open(file_path, 'rb') as file_con:
        compressed_data = lzma.compress(file_con.read())
        

    # Send content in pieces of 512 Kilobytes
    parts = math.ceil(len(compressed_data)/(512*1024))
    hashes = []

    for ck in tqdm(range(parts), ascii=True, desc=file_name):
        # Send the chunk and its md5sum
        chunk = compressed_data[ck*(512*1024):(ck + 1)*(512*1024)]
        hash = str(md5(chunk).hexdigest())
        hashes.append(hash)

        try:
            chunks.put_object(Key=hash, Body=chunk)
        except Exception as e:
            print(e)

    try:
        files.put_object(
            Key=file_name,
            Body=bytes('\n'.join(hashes), 'utf-8')
        )
    except Exception as e:
        print(e)


def get_file(file_name):
    content = BytesIO()
    try:
        files.download_fileobj(Key=file_name, Fileobj=content)
    except Exception as e:
        print(e)
        return

    # Get each chunk
    hashes = content.getvalue().split(b'\n')
    content = b''
    for c in hashes:
        chunk = BytesIO()
        chunks.download_fileobj(Key=c.decode('ascii'), Fileobj=chunk)
        content += chunk.getvalue()

    # Collect chunk's contents
    os.write(1, lzma.decompress(content))

    print("")


def list_remote_files():
    for fi in files.objects.all():
        print(fi.key)


# Main execution
if len(sys.argv) == 1:
    print(__doc__)
    sys.exit(1)

configure()
connect()
if sys.argv[1] == 'list':
    list_remote_files()
elif sys.argv[1] == 'get':
    get_file(sys.argv[2])
elif sys.argv[1] == 'send':
    y = False
    name = None
    path = ""
    i = 1
    while i <= len(sys.argv[2:]):
        i += 1
        if sys.argv[i] == '-n':
            if sys.argv[i+1:] == []:
                print("error: missing name argument")
                print(__doc__)
                sys.exit(2)
            name = sys.argv[i+1]
            i += 1
        elif sys.argv[i] == '-y':
            y = True
        else:
            path = sys.argv[i]
    if path == "":
        print("error: missing filename argument")
        print(__doc__)
        sys.exit(3)
    send_file(path, name, y)
else:
    print('invalid command:', sys.argv[1])
    print(__doc__)
    sys.exit(-1)
