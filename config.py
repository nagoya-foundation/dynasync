"""
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
    files.

    AUTHOR: Brian Mayer

    DESCRIPTION: Configuration file for dynasync. Here we store the client's
    preferences, such as remote db name and tracked directories.
"""

import os
import json

# Now we create the .config directory if needed
config_dir = os.path.expanduser("~/.config/dynasync")
if not os.path.exists(config_dir):
	os.makedirs(config_dir)
    print("~/.config/dynasync directory created.")

# Check if config file exists in that dir
config_file = config_dir + "/dynasync.conf"
configs = {}
if os.path.exists(config_file):
    print("Config file already exists.")
    with open(config_file, 'r') as file:
        configs = json.load(file)
else:
    # Get configuration parameters from user and write to file
    configs['dir'] = input("Enter directory to track:")
    configs['table'] = input("Enter DynamoDB table name:")

    with open(config_file, 'w') as file:
        file.write(json.dumps(configs))

print("Configuration finished!")
