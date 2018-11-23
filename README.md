# dynasync

> A project to sincronize local files with AWS DynamoDB service. Here we implement a very simple versioning system, barely with commit and tag only. The intention is to simplify workflow by providing an easy to learn solution.

## Prerequisites

- go

## Building

Run `go install` inside the *src/dynasync* folder. You may want to move the generated binary to a location in your `$PATH`. 

## Configuring

First, read the master branch README.md, then create a folder and enter it, finally run the command `dynasync init [name]`, `name` is optional, by default it will use the current folder name.

## Meta

Created by: Brian Mayer - bleemayer@gmail.com 

Initial commit: November, 23, 2018 

Distributed under The BSD3-clause license. See [LICENSE](LICENSE) for more information.

### Current work

- Creating the commit function
- Implementing connection with AWS DynamoDB

### Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it is pretty intuitive and straightforward.
