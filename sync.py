# Function to initialize local indexes
def init():
    # Put all files in a list
    print("Collecting files under " + configs['dir'] + "...")
    tracked_files = []
    collect_files(configs['dir'], tracked_files)

    # Create a dict for each file
    locs = {}

    # Append file information like size, mtime &c
    print("Creating files attributes...")
    for file in tracked_files:
        locs[file] = {
            'mtime': os.path.getmtime(file),
            'size': os.path.getsize(file),
            'deleted': "False"
        }
        print("File " + file + " processed.")

    # Get remote tracked files
    print("Querying files in remsote table.")
    table_files = dynamo.scan(
        TableName = configs['table'],
        FilterExpression = '#del = :a',
        ExpressionAttributeNames = {
            '#fp': 'filePath',
            '#mt': 'mtime',
            '#del': 'deleted'
        },
        ExpressionAttributeValues = {
            ':a': {'B': 'False'}
        },
        ProjectionExpression = '#fp, #mt'
    )['Items']

    # Reformat into a dictionary
    rems = {}
    for file in range(len(table_files)):
        rems[table_files[file]['filePath']['S']] = {
            'mtime': float(table_files[file]['mtime']['S'])
        }

    coms = set(locs).intersection(set(rems))
    mods = {x for x in coms if locs[x]['mtime'] > rems[x]['mtime']}

    # Find existing but modified files
    if len(mods) == 0:
        print("No modified files.")
    else:
        print("Modified files to be uploaded:")
        print(mods)

    # Find new local files
    new_files = locs.keys() - rems.keys()
    if len(new_files) == 0:
        print("No new files.")
    else:
        print("New files to be uploaded:")
        print(new_files)

    # Insert them in the remote table
    for newFile in mods.union(new_files):
        print("Uploading file " + newFile)
        with open(newFile, 'rb') as file:
            fileBytes = file.read()
            content = str(lzma.compress(fileBytes))
            if len(content) > 399900:
                print("File too large (> 400 KiB) for DynamoDB, skipping...")
                continue
            
            dynamo.put_item(
                TableName = 'docs',
                Item = {
                    'filePath': {'S': newFile},
                    'mtime': {'S': str(locs[newFile]['mtime'])},
                    'hash': {'S': hashlib.md5(fileBytes).hexdigest()},
                    'size': {'N': str(locs[newFile]['size'])},
                    'content': {'S': content},
                    'chunked': {'B': "False"},
                    'deleted': {'B': "False"}
                }
            )


