package main

import (
	"fmt"
// 	"os"
// 	"path/filepath"
// 	"crypto/md5"
// 	"encoding/base64"
)

// TODO: Add glob support
// TODO: Open an editor to enter message
func get() {
	commits, err := getAllCommits(REPONAME)
	newFiles, err := getFileChanges(commits)
	if err != nil {
		fmt.Println("error getting commits: " + err.Error())
	}

	fmt.Println(newFiles)
	return
}

