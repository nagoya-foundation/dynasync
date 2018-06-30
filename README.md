# dynasync

> dynasync uses the low-level AWS API to send the files under selected folders to the DynamoDB service. It will send and store it a secure way. So you have versioning for you files for free, since that AWS offers 25GiB DynamoDB storage as a free service.

## Configuring

First you need an AWS account with the DynamoDB service available and the AWS's credentials in your home folder.

## Requisites

- Python 3.6
- boto3 package

To install Python please visit its [Official Website](https://www.python.org/) and simply follow the instructions. The you can install boto3 package with: `pip install -r requirements.txt`. And you're good to go.

## Running dynasync

Simply issue the command: `python3 main.py`, it will run a setup script and then start monitoring the files you selected.

## Meta

Created by: Brian Mayer - bleemayer@gmail.com	
Initial commit: May, 15, 2018
Distributed under The BSD3-clause license. See [LICENSE](LICENSE) for more information.

### Current work

- Adding support for larger files and more than one folder.

### To-do

There are lots of things to do, the ones in my mind now are listed below.

- Create a nice user interface for configuring the tool.

### Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it is pretty intuitive and straightforward.

