""" 
    PROJECT: dynasync: An utility that uses AWS's DynamoDB to store your 
    files.
    
    AUTHOR: Brian Mayer
    
    DESCRIPTION: Configuration file for dynasync. Here we store the client's
    preferences, such as remote db name and tracked directories. 
"""

import pathlib
import json

# Now we create the .config directory if needed
config_dir = pathlib.Path().home().joinpath(".config/dynasync")
if not config_dir.exists():
    config_dir.mkdir(parents=True)
    print(".config/dynasync directory created.")

# Check if config file exists in that dir
config_file = config_dir.joinpath("dynasync.conf")
configs = {}
if config_file.exists():
    print("Config file already exists.")
    with open(config_file, 'r') as file:
        configs = json.load(file)
else:
    # Create the config file
    config_file.touch()
    print("dynasync.conf file created.")

    # Get configuration parameters from user and write to file
    configs['dir'] = input("Enter directory to track:")
    configs['table'] = input("Enter DynamoDB table name:")

    with open(config_file, 'w') as file:
        file.write(json.dumps(configs))

print("Configuration finished!")

