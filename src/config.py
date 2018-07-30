# -------------------------------------------------------------------------- #
# Dynasync condfiguration file. Read or write configs and connect to the given
# DynamoDB table
# -------------------------------------------------------------------------- #

import os
import time
import json
import boto3
import argparse

# ---- Command line arguments ----------------------------------------------- #

print("Dynasync starting " + time.ctime())
parser = argparse.ArgumentParser()
parser.add_argument("--reconf", help = "call configure", action = "store_true")
args = parser.parse_args()

# ---- Configure ------------------------------------------------------------ #

# Now we create the .config directory if needed
config_dir = os.path.expanduser("~/.config/dynasync")
if not os.path.exists(config_dir):
    os.makedirs(config_dir)
    print("~/.config/dynasync directory created.")

# Check if config file exists in that dir
config_file = config_dir + "/dynasync.conf"
if not os.path.exists(config_file) or args.reconf:
    # Get configuration parameters from user and write to file
    print("Creating configuration file.")
    track_dirs = input("Enter directory to track:")
    dyna_table = input("Enter DynamoDB table name:")
    with open(config_file, 'w') as file:
        file.write(json.dumps({'dir': track_dirs, 'table': dyna_table}))

with open(config_file, 'r') as file:
    configs = json.load(file)
    track_dirs = configs['dir']
    dyna_table = configs['table']

print("Configuration finished!\nChecking if it is valid.")
if not os.path.exists(track_dirs):
    print("Entered directory does not exists! Program will fail.")
    exit(-1)

