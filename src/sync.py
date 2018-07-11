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
    # Open file and compress its contents 
    with open(file, 'rb') as file_con:
        fileBytes = file_con.read()
        content = lzma.compress(fileBytes)

    if len(content) > 50*(2**20):
        print("File " + file + " is too large (> 50 MiB), skipping...")
        return 0
    
    # Send content in pieces of 256KiB
    chunks = math.ceil(len(content)/2**18)
    hashes = []
    ck = 0
    while ck < chunks:
        # Send the chunk and its sha1
        print("Sending " + file + "\t" + str(ck + 1) + "/" + str(chunks))
        chunk = content[ck*(2**18):(ck + 1)*(2**18)]
        hash = hashlib.sha1(chunk).hexdigest()
        hashes.append(hash)
        ck += 1
        
        # Try to send the chunk
        try:
            table.put_item(
                Item = {
                    'chunkid': hash,
                    'content': chunk
                },
                ConditionExpression = 'attribute_not_exists(chunkid)'
            )
            
            # Wait a sec to preserve throughput
            time.sleep(1)
        except:
            print('Chunk already exists or error happened.')
            continue
        
        # Update index regardless of the size
        update_index(table, index, file, hashes)


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
            '#fp': 'filepath',
            '#cl': 'chunks',
            '#mt': 'mtime'
        },
        ExpressionAttributeValues = {
            ':a': False,
            ':t': dyna_table.name
        },
        ProjectionExpression = '#fp, #mt, #cl',
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

