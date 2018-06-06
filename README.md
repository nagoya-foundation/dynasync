# dynasync

> dynasync uses the low-level AWS API to send the files under selected folders to the DynamoDB service. It will send and store it a secure way. So you have versioning for you files for free, since that AWS offers 25GiB DynamoDB storage as a free service.

# Configuring

First you need an AWS account with the DynamoDB service available and the AWS's credentials in your home folder.

# Running dynasync

Simply issue the command: `python3 main.py`, it will run a setup script and then start monitoring the files you selected.

# Meta

Created by: Brian Mayer - bleemayer@gmail.com	
Initial commit: May, 15, 2018
Distributed under The BSD3-clause license. See [LICENSE](LICENSE) for more information.

## Current work

- Table creation; &
- Verification logic.

## To-do

There are lots of things to do, the ones in my mind now are listed below.

- Create a nice user interface for configuring the tool.

## Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it is pretty intuitive and straightforward.

