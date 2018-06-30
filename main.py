#! /usr/bin/env python3

"""
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your
    files.

    AUTHOR: Brian Mayer

    DESCRIPTION: Main file for dynasync, it uses the AWS low-level API to make
    requests to DynamoDB. We try to make it as efficient as possible with the
    help of hashing and modification time to only synchronize new files.
"""

import sync

# ---- Program execution ---------------------------------------------------- #

# Run initialization
sync.init()

# Keep walking
print("Synchronized, watching for changes...") 
try:
    sync.watch()
except KeyboardInterrupt:
    print('Interrupted')
    exit(0)


# TODO: Use dir as parameter for init and watch

# TODO: Update deleted files

