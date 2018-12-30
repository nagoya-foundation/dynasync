package main

import (
	"fmt"
	"os"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func getFileChanges(commits []Commit) (map[string]string, error) {
	files := map[string]string{}
	temp := map[string][]diffmatchpatch.Patch{}

	// Create diff to send
	dmp := diffmatchpatch.New()
	fmt.Println("creating backward patch...")
	for _, commit := range commits {
		fmt.Println("Applying commit " + string(commit.Hash[0:16]))
		patches, err := dmp.PatchFromText(commit.Diff)
		if err != nil {
			return nil, err
		}

		for _, patch := range patches {
			temp[commit.File] = append(temp[commit.File], patch)
		}

		newFile, _ := dmp.PatchApply(patches, files[commit.File])
		files[commit.File] = newFile
	}

	return files, nil
}

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
	commits, _ := getAllCommits(REPONAME)

	// Take all diffs
	newFiles, err := getFileChanges(commits)
	if err != nil {
		fmt.Println("error applying commits: " + err.Error())
	}

	for id, file := range newFiles {
		fileConn, _ := os.Create(id)
		_, err = fileConn.Write([]byte(file))
		if err != nil {
			fmt.Println("error writing " + id + ": " + err.Error())
		}
		fileConn.Close()
	}
	fmt.Println("done")

	return
}

