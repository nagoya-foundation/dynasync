# --------------------------------------------------------------------------- #
# Function definitions for dynasync
# 
# Copyright Nagoya Foundation 
# --------------------------------------------------------------------------- #

from decimal import *
import math
import os
import time
import hashlib
import lzma
import tqdm
import _thread

# Send a file to remote table
def send_file(table, index, root, file):
    # Expand ~ in file path file
    fpath = os.path.join(root, file)

    # Verify file size
    if os.path.getsize(fpath) > 104857600:
        print("File " + file + " is too large (> 100 MiB), skipping...")
        return 0

    # Get modification time for update_index
    mtime = os.path.getmtime(fpath)

    # Open file and compress its contents 
    with open(fpath, 'rb') as file_con:
        fileBytes = file_con.read()
        content = lzma.compress(fileBytes)

    # Send content in pieces of 256KiB
    chunks = math.ceil(len(content)/262144)
    hashes = []
    ck = 0
    for ck in tqdm.trange(chunks, ascii=True, desc="Sending " + os.path.basename(file)):
        # Send the chunk and its sha1
        chunk = content[ck*262144:(ck + 1)*262144]
        hash = hashlib.sha1(chunk).hexdigest()
        hashes.append(hash)
        ck += 1

        # Try to send the chunk
        try:
            new_item = table.put_item(
                Item = {
                    'chunkid': hash,
                    'content': chunk
                },
                ConditionExpression = 'attribute_not_exists(chunkid)',
                ReturnConsumedCapacity = 'TOTAL'
            )

            # Wait a sec to preserve throughput
            if new_item['ConsumedCapacity']['CapacityUnits'] > 24:
                time.sleep(new_item['ConsumedCapacity']['CapacityUnits']/24)

        except:
            pass

        # Update index regardless of the size
        update_index(table, index, file, hashes, mtime)

def update_index(table, index, file, chunk_list, mtime):
    index.update_item(
        Key = {
            'dyna_table': table.name,
            'filepath': file
        },
        UpdateExpression = "set mtime = :r, deleted = :s, chunks = :cl",
        ExpressionAttributeValues = {
            ':r': Decimal(mtime),
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
            ':r': mtime,
            ':s': True
        }
    )

def get_file(table, root, file, chunks):

    # Initialize empty file contents variable
    content = b''

    # Get each chunk
    for i in tqdm.trange(len(chunks), ascii=True, desc="Getting " + os.path.basename(file)):
        try:
            new_file = table.get_item(
                Key = {
                    'chunkid': chunks[i]
                },
                ReturnConsumedCapacity = 'TOTAL'
            )
        except KeyboardInterrupt:
            exit()

        except:
            print("Chunk " + chunks[i] +  " not found!")
            pass

        # Collect chunk's contents
        content += new_file['Item']['content'].value

        # Wait based on consumed capacity
        if new_file['ConsumedCapacity']['CapacityUnits'] > 15:
            time.sleep(new_file['ConsumedCapacity']['CapacityUnits']/15)

    # Write file to disk
    os.makedirs(os.path.dirname(os.path.join(root, file)), exist_ok=True)
    with open(os.path.join(root, file), 'wb') as file_df:
        file_df.write(lzma.decompress(content))
        file_df.close()

# Get all files under the selected dir
def collect_files(root_dir, dir, files):
    # List files in current directory
    dir_files = os.listdir(dir)

    # Search through all files
    for filename in dir_files:
        filepath = os.path.join(dir, filename)

        # Check if it's a normal file or directory
        if os.path.isfile(filepath):
            path = os.path.relpath(filepath, root_dir)
            files.append(path)
        else:
            # We got a directory, enter it for further processing
            collect_files(root_dir, filepath, files)

# Function to initialize local indexes
def init(dyna_table, dyna_index, track_dirs):
    paths = os.path.expanduser(track_dirs)

    # Put all files in a list
    print("Collecting files under " + track_dirs + "...")
    local_files = []
    collect_files(paths, paths, local_files)
    print(str(len(local_files)) + " files found.")

    # Get remote tracked files
    print("Querying files in remote table.")
    file_count = 0
    scan_result = dyna_index.scan(
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
        FilterExpression = 'deleted = :a and dyna_table = :t',
        ReturnConsumedCapacity = 'TOTAL'
    )
    table_files = scan_result['Items']
    file_count += scan_result['Count']

    # Keep scanning until all results are received
    while 'LastEvaluatedKey' in scan_result.keys():
        # Wait if needed
        if scan_result['ConsumedCapacity']['CapacityUnits'] > 10:
            time.sleep(scan_result['ConsumedCapacity']['CapacityUnits']/10)

        # Scan from the last result
        scan_result = dyna_index.scan(
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
            FilterExpression = 'deleted = :a and dyna_table = :t',
            ExclusiveStartKey = scan_result['LastEvaluatedKey'],
            ReturnConsumedCapacity = 'TOTAL'
        )

        # Merge all items
        table_files.extend(scan_result['Items'])
        file_count += scan_result['Count']

    # Reformat into a dictionary
    print(str(file_count) + " items read.")
    remote_files = []
    rems = {}
    for file in range(file_count):
        remote_files.append(table_files[file]['filepath'])
        rems[table_files[file]['filepath']] = {
            'mtime': table_files[file]['mtime'],
            'chunks': table_files[file]['chunks']
        }

    # Download remote only files
    for file in set(remote_files) - set(local_files):
        get_file(dyna_table, paths, file, rems[file]['chunks'])

    # Insert modified files in the remote table
    for file in local_files:
        f = os.path.join(paths, file)
        if file not in remote_files or os.path.getmtime(f)>rems[file]['mtime']:
            send_file(dyna_table, dyna_index, paths, file)


def resolve_diff(dyna_table, dyna_index, root, olds, news, mtime):
    # Send modified files
    for file in news:
        if os.path.getmtime(os.path.join(root, file)) > mtime:
            send_file(dyna_table, dyna_index, root, file)

    # Update deleted files
    for file in set(olds) - set(news):
        set_deleted(dyna_table, dyna_index, file, mtime)

    # Send new files
    for file in set(news) - set(olds):
        send_file(dyna_table, dyna_index, root, file)

# Keep track of files
def watch(dyna_table, dyna_index, track_dirs):

    # Recollect files
    olds = []
    news = []
    paths = os.path.expanduser(track_dirs)
    collect_files(paths, paths, olds)

    # Loop forever
    while True:
        # Get the bigger mod time
        maxm = max([os.path.getmtime(os.path.join(paths, x)) for x in olds])

        # Wait some time
        time.sleep(7)

        # Recollect files
        news = []
        collect_files(paths, paths, news)

        # Start a tread to resolve and send updates
        _thread.start_new_thread(
                resolve_diff,
                (dyna_table, dyna_index, paths, olds, news, maxm)
                )
        olds = news

