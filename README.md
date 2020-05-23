# dynasync

> This program will send, list or receive files to an object storage and store it securely.


## Configuring

Before running you must define two environment variables: DYNA_KEY and DYNA_SECRET with you key id and secret values, also, change the url in the code.


## Requisites

- Python > 3
- pip

To install Python please visit its [Official Website](https://www.python.org/) and simply follow the instructions. Then you can install boto3 package with: `pip install -r requirements.txt`. And you're good to go.


## Running dynasync


For a better experience move the src/main.py file to your path with a name you prefer.


### Sending a file

Run `./main.py send <file>`, passing the path to the file, before sending, the program will append the relative path from your home directory.


### Getting a file

Run `./main.py get <file>`, passing the path to the file, it will print the file's content to terminal, you may use redirection to create a file.


### Listing remote files

Run `./main.py list` and the program will print the size, in chunks, and the names to terminal.


## Meta

Created by: Brian Mayer - bleemayer@gmail.com
Initial commit: August 27, 2018
Distributed under BSD3-clause license. See [LICENSE](LICENSE) for more information.


### To-do

There are lots of things to do, the ones in my mind now are listed below.

- Create a front-end program


### Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it is pretty intuitive and straightforward.

