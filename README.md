# dynasync

> In this branch we use an expoxed API to send the files under selected folders to the DynamoDB service. The program will send or receive files to a lambda function using the API Gateway AWS service and store it a secure way. So you have versioning for your files for free, since that AWS offers 25GiB DynamoDB storage as a free service.

This new version permits multiple conections at the same time, so it is a new step in the development of this helpfull tool.

## Configuring

In this branch you only need you API key from the API Gateway service.

## Requisites

- Python > 3.6
- pip

To install Python please visit its [Official Website](https://www.python.org/) and simply follow the instructions. The you can install boto3 package with: `pip install -r requirements.txt`. And you're good to go.

## Running dynasync

Simply issue the command: `./main.py`, it will run a setup script and then start monitoring the files you selected.

## Meta

Created by: Brian Mayer - bleemayer@gmail.com
Initial commit: August, 27, 2018
Distributed under The BSD3-clause license. See [LICENSE](LICENSE) for more information.

### Current work

- Creating the back-end lambda function
- Adding support for more than one folder tracking

### To-do

There are lots of things to do, the ones in my mind now are listed below.

- Create o front-end program
- Create a nice user interface for configuring the front-end

### Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it is pretty intuitive and straightforward.

