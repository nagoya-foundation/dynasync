# dynasync

> A project to synchronize local files with AWS DynamoDB service. Here we
implement a very simple versioning system, barely with commit and tag only.
The intention is to simplify work flow by providing an easy to learn solution.

## Prerequisites

- go

## Building

Run `go install` inside the *src/dynasync* folder. You may want to move the
generated binary to a location in your `$PATH`.

## Configuring

You must create two tables in DynamoDB and grant access to the user you are
use in your local AWS configuration, those two tables must be as follows:

- Table "commits":
  - Hash key: "repo", type string
  - Range key: "commitDate", type number

- Table "repos":
  - Hash key: "repo", type string

The capacity is up to you to decide

Then you can run the command `dynasync init [name]`, `name` is optional, by
default it will use the current folder name, to start a dynasync repo on that
folder.

## Meta

Created by: Brian Mayer - bleemayer@gmail.com

Initial commit: November, 23, 2018

Distributed under The BSD3-clause license. See [LICENSE](LICENSE) for more
information.

### Current work

- Creating the commit function
- Implementing connection with AWS DynamoDB

### Contributing

Check the [contributing](CONTRIBUTING.md) file for details, but, in advance, it
is pretty intuitive and straightforward.
