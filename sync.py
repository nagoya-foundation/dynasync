# --------------------------------------------------------------------------- #
# Function definitions for dynasync
# --------------------------------------------------------------------------- #

import config
import os
import boto3
import time
import hashlib
import lzma
import _thread

# Send a file to remote table
def send_file(file):
    with open(file, 'rb') as file_con:
        fileBytes = file_con.read()
        content = lzma.compress(fileBytes)
        if len(content) > 399900:
            print("File too large (> 400 KiB) for DynamoDB, skipping...")
        else: 
            config.dynamo.put_item(
                TableName = config.dyna_table,
                Item = {
                    'deleted': {'S': 'False'},
                    'filePath': {'S': file},
                    'mtime': {'S': str(os.path.getmtime(file))},
                    'hash': {'S': hashlib.md5(fileBytes).hexdigest()},
                    'size': {'N': str(os.path.getsize(file))},
                    'content': {'B': content},
                    'chunked': {'BOOL': False}
                }
            )
            update_index(file, False)

def update_index(file, status):
    config.dynamo.update_item(
        TableName = config.dyna_index,
        Key = {
            'dyna_table': {'S': config.dyna_table},
            'filePath': {'S': file}
        },
        UpdateExpression = "set mtime = :r, deleted = :s",
        ExpressionAttributeValues = {
            ':r': {'S': str(os.path.getmtime(file))},
            ':s': {'BOOL': status}
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
def init():
    # Put all files in a list
    print("Collecting files under " + config.track_dirs + "...")
    local_files = []
    remote_files = []
    rems = {}
    collect_files(config.track_dirs, local_files)

    # Get remote tracked files
    print("Querying files in remote table.")
    table_files = config.dynamo.scan(
        TableName = config.dyna_index,
        ExpressionAttributeNames = {
            '#fp': 'filePath',
            '#mt': 'mtime'
        },
        ExpressionAttributeValues = {
            ':a': {'BOOL': False},
            ':t': {'S': config.dyna_table}
        },
        ProjectionExpression = '#fp, #mt',
        FilterExpression = 'deleted = :a and dyna_table = :t'
    )['Items']

    # Reformat into a dictionary
    for file in range(len(table_files)):
        remote_files.append(table_files[file]['filePath']['S'])
        rems[table_files[file]['filePath']['S']] = {
            'mtime': float(table_files[file]['mtime']['S'])
        }

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        print("Downloading remote file " + file)
        new_file = config.dynamo.get_item(
            TableName = config.dyna_table,
            Key = {
                'filePath': {'S': file},
                'mtime': {'S': str(rems[file]['mtime'])}
            }
        )
        with open(file, 'wb') as file_df:
            file_df.write(new_file['Item']['content']['B'])
            file_df.close()

    # Insert modified files in the remote table
    for file in local_files:
        if file not in remote_files or os.path.getmtime(file) > rems[file]['mtime']:
            print("Sending file " + file + "...")
            send_file(file)


def resolve_diff(olds, news, mtime):
    # Send modified files
    for file in news:
        if os.path.getmtime(file) > mtime:
            print("File " + file + " modified, sending.")
            send_file(file)

    # Update deleted files
    for file in set(olds) - set(news):
        print("File " + file + " deleted, updating.")
        update_index(file, True)

# Keep track of files
def watch():

    # Recollect files
    old_files = []
    new_files = []
    collect_files(config.track_dirs, old_files)

    # Loop forever
    while True:
        # Get the bigger mod time
        max_mod = max([os.path.getmtime(x) for x in old_files]) 
        
        # Wait some time
        time.sleep(10)
        
        # Recollect files
        new_files = []
        collect_files(config.track_dirs, new_files)
        
        # Start a tread to resolve and send updates
        _thread.start_new_thread(resolve_diff, (old_files, new_files, max_mod))
        old_files = new_files









