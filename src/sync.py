# --------------------------------------------------------------------------- #
# Function definitions for dynasync
# 
# Copyright Nagoya Foundation 
# --------------------------------------------------------------------------- #

import math
import os
import time
import hashlib
import lzma
import _thread

# Send a file to remote table
def send_file(table, index, file):
    if os.path.getsize(file) > 50*(2**20):
        print("File " + file + " is too large (> 50 MiB), skipping...")
    else:
        with open(file, 'rb') as file_con:
            fileBytes = file_con.read()
            content = lzma.compress(fileBytes)
            hashes = []
            if len(content) > 399900:
                # Send content in pieces
                chunks = math.ceil(len(content)/399900)
                print("Sending " + file + " in " + str(chunks) + " parts.")
                ck = 0
                while ck < chunks:
                    print("Part " + str(ck + 1) + " of " + str(chunks))
                    chunk = content[ck*399900:(ck + 1)*399900]
                    hash = hashlib.sha1(chunk).hexdigest()
                    hashes.append(hash)
                    ck += 1
                    try:
                        table.put_item(
                            Item = {
                                'chunkid': hash,
                                'content': chunk
                            },
                            ConditionExpression='attribute_not_exists(chunkid)'
                        )
                    except:
                        print('Chunk already exists or error happened.')
                        continue
                    time.sleep(2)
            else:
                print("Sending file " + file + "...")
                hashes.append(hashlib.sha1(content).hexdigest())
                try:
                    table.put_item(
                        Item = {
                            'chunkid': hashes[0],
                            'content': content
                        },
                        ConditionExpression = 'attribute_not_exists(chunkid)'
                    )
                except:
                    print('Chunk already exists or error happened.')
            
            # Update index regardless of the size
            update_index(table, index, file, hashes)
            
            # Wait a sec to preserve throughput
            time.sleep(1)

def get_file(table, file, files, mtime):

    # Initialize empty file contents variable
    content = b''

    # Get each chunk
    for chunk in files:
        print("Downloading " + files + ".")
        new_file = table.get_item(
            Key = {
                'chunkid': files
            }
        )
        # Collect chunk's contents
        content += new_file['Item']['content']

    # Write file to disk
    with open(file, 'wb') as file_df:
        file_df.write(content)
        file_df.close()

def update_index(table, index, file, chunk_list):
    index.update_item(
        Key = {
            'dyna_table': table.name,
            'filepath': file
        },
        UpdateExpression = "set mtime = :r, deleted = :s, chunks = :cl",
        ExpressionAttributeValues = {
            ':r': str(os.path.getmtime(file)),
            ':s': False,
            ':cl': chunk_list
        }
    )

def set_deleted(table, index, file, mtime):
    print("File " + file + " deleted, updating.")
    index.update_item(
        Key = {
            'dyna_table': table.name,
            'filepath': file
        },
        UpdateExpression = "set mtime = :r, deleted = :s",
        ExpressionAttributeValues = {
            ':r': str(mtime),
            ':s': True
        }
    )

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

# Function to initialize local indexes
def init(dyna_table, dyna_index, track_dirs):
    # Put all files in a list
    print("Collecting files under " + track_dirs + "...")
    local_files = []
    remote_files = []
    rems = {}
    collect_files(track_dirs, local_files)
    print(str(len(local_files)) + " files found.")

    # Get remote tracked files
    print("Querying files in remote table.")
    table_files = dyna_index.scan(
        ExpressionAttributeNames = {
            '#mt': 'mtime',
            '#cl': 'chunks'
        },
        ExpressionAttributeValues = {
            ':a': False,
            ':t': dyna_table.name
        },
        ProjectionExpression = '#mt, #cl',
        FilterExpression = 'deleted = :a and dyna_table = :t'
    )['Items']

    # Reformat into a dictionary
    nitem = len(table_files)
    print(str(nitem) + " items read.")
    for file in range(nitem):
        remote_files.append(table_files[file]['filepath'])
        rems[table_files[file]['filepath']] = {
            'mtime': float(table_files[file]['mtime']),
            'chunks': table_files[file]['chunks']
        }

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        get_file(dyna_table, file, rems[file]['chunks'], rems[file]['mtime'])

    # Insert modified files in the remote table
    for f in local_files:
        if f not in remote_files or os.path.getmtime(f) > rems[f]['mtime']:
            send_file(dyna_table, dyna_index, f)


def resolve_diff(dyna_table, dyna_index, olds, news, mtime):
    # Send modified files
    for file in news:
        if os.path.getmtime(file) > mtime:
            send_file(dyna_table, dyna_index, file)

    # Update deleted files
    for file in set(olds) - set(news):
        set_deleted(dyna_table, dyna_index, file, mtime)

    # Send new files
    for file in set(news) - set(olds):
        send_file(dyna_table, dyna_index, file)

# Keep track of files
def watch(dyna_table, dyna_index, track_dirs):

    # Recollect files
    olds = []
    news = []
    collect_files(track_dirs, olds)

    # Loop forever
    while True:
        # Get the bigger mod time
        maxm = max([os.path.getmtime(x) for x in olds]) 
        
        # Wait some time
        time.sleep(7)
        
        # Recollect files
        news = []
        collect_files(track_dirs, news)
        
        # Start a tread to resolve and send updates
        _thread.start_new_thread(
            resolve_diff,
            (dyna_table, dyna_index, olds, news, maxm)
        )
        olds = news

