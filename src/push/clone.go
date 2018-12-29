package main

import (
	"fmt"
	"os"
)

func clone(repo string) {
	// Start creating the repo
	err := os.MkdirAll(repo, 0777)
	if err != nil {
		panic("error creating repo folder")
	}

	// Go inside folder and run initRepo
	err = os.Chdir(repo)
	if err != nil {
		panic("Could not change to repo's dir: " + err.Error())
	}

	// Redefine global variables
	REPOPATH, _ = os.Getwd()
	initRepo(repo)

	// Download files from table
	commits, err := getAllCommits(REPONAME)
	if err != nil {
		fmt.Println("error getting all commits: " + err.Error())
	}
	fmt.Println(commits)

	return
}

