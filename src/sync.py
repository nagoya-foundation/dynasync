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
    if os.path.getsize(file) > 100*(2**20):
        print("File " + file + " is too large (> 100 MiB), skipping...")
    else:
        with open(file, 'rb') as file_con:
            fileBytes = file_con.read()
            content = lzma.compress(fileBytes)
            if len(content) > 399900:
                # Send content in pieces
                chunks = math.ceil(len(content)/399900)
                print("Sending " + file + " in " + str(chunks) + " parts.")
                ck = 0
                while ck < chunks:
                    print("Part " + str(ck + 1) + " of " + str(chunks))
                    table.put_item(
                        Item = {
                            'filepath': file,
                            'deleted': 'False',
                            'chunkid': str(os.path.getmtime(file))+'/'+str(ck),
                            'hash': hashlib.md5(fileBytes).hexdigest(),
                            'mtime': str(os.path.getmtime(file)),
                            'content': content[ck*399900:(ck + 1)*399900],
                            'chunks': chunks,
                            'chunk': ck
                        }
                    )
                    ck += 1
                    time.sleep(2)
            else:
                print("Sending file " + file + "...")
                table.put_item(
                    Item = {
                        'filepath': file,
                        'deleted': 'False',
                        'chunkid': str(os.path.getmtime(file)) + '/0',
                        'mtime': str(os.path.getmtime(file)),
                        'hash': hashlib.md5(fileBytes).hexdigest(),
                        'content': content,
                        'chunks': 1,
                        'chunk': 0
                    }
                )
            # Update index regardless of the size
            update_index(table, index, file, False)
            
            # Wait a sec to preserve throughput
            time.sleep(1)

def get_file(table, file, mtime):
    # Get first chunk
    print("Downloading " + file + ".")
    new_file = table.get_item(
        Key = {
            'filepath': file,
            'mtime': str(mtime),
            'chunk': 0
        }
    )
    content = new_file['Item']['content']

    # Get other chunks if any
    chunk = 1
    chunks = new_file['Item']['chunks']
    while chunk < chunks:
        print("Part " + str(chunk) + " of " + str(chunks))
        new_file = table.get_item(
            Key = {
                'filepath': file,
                'mtime': str(mtime),
                'chunk': chunk
            }
        )
        content += new_file['Item']['content']
        chunk += 1

    # Write file to disk
    with open(file, 'wb') as file_df:
        file_df.write(content)
        file_df.close()

def update_index(table, index, file, status):
    index.update_item(
        Key = {
            'dyna_table': table.name,
            'filepath': file
        },
        UpdateExpression = "set mtime = :r, deleted = :s",
        ExpressionAttributeValues = {
            ':r': str(os.path.getmtime(file)),
            ':s': status
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

    # Get remote tracked files
    print("Querying files in remote table.")
    table_files = dyna_index.scan(
        ExpressionAttributeNames = {
            '#fp': 'filepath',
            '#mt': 'mtime'
        },
        ExpressionAttributeValues = {
            ':a': False,
            ':t': dyna_table.name
        },
        ProjectionExpression = '#fp, #mt',
        FilterExpression = 'deleted = :a and dyna_table = :t'
    )['Items']

    # Reformat into a dictionary
    for file in range(len(table_files)):
        remote_files.append(table_files[file]['filepath'])
        rems[table_files[file]['filepath']] = {
            'mtime': float(table_files[file]['mtime'])
        }

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        get_file(dyna_table, file, rems[file]['mtime'])

    # Insert modified files in the remote table
    for f in local_files:
        if f not in remote_files or os.path.getmtime(f) > rems[f]['mtime']:
            send_file(dyna_table, dyna_index, f)


def resolve_diff(dyna_table, dyna_index, olds, news, mtime):
    # Send modified files
    for file in news:
        if os.path.getmtime(file) > mtime:
            print("File " + file + " modified, sending.")
            send_file(dyna_table, dyna_index, file)

    # Update deleted files
    for file in set(olds) - set(news):
        print("File " + file + " deleted, updating.")
        update_index(dyna_table, dyna_index, file, True)

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
        time.sleep(10)
        
        # Recollect files
        news = []
        collect_files(track_dirs, news)
        
        # Start a tread to resolve and send updates
        _thread.start_new_thread(
            resolve_diff,
            (dyna_table, dyna_index, olds, news, maxm)
        )
        olds = news

