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
		panic("could not change to repo's dir: " + err.Error())
	}

	// Redefine global variables
	REPOPATH, _ = os.Getwd()
	initRepo(repo)

	// Download files from table
	commitIds, err := listRepoCommits()
	if err != nil {
		fmt.Println("error getting commits: " + err.Error())
		return
	}

	for _, commit := range commitIds {
		// Apply commit
		err = applyCommit(commit)
		if err != nil {
			fmt.Println("error writing commit: " + err.Error())
		}
	}
	fmt.Println("done")

	return
}

