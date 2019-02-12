package main

import (
	"encoding/base64"
	"fmt"
	"github.com/sergi/go-diff/diffmatchpatch"
	"os"
)

func applyCommit(commit Commit) error {
	dmp := diffmatchpatch.New()

	// Get commit from remote table
	commit, err := getCommit(commit.Date)
	patches, err := dmp.PatchFromText(commit.Diff)
	if err != nil {
		return err
	}

	// Open file and read contents
	fileInfo, err := os.Stat(commit.File)
	var fileContent []byte
	fileConn, _ := os.OpenFile(commit.File, os.O_RDWR|os.O_CREATE, 0666)

	if err == nil {
		fileContent = make([]byte, fileInfo.Size())
		fileConn.Read(fileContent)
	} else {
		fileContent = make([]byte, 0)
	}

	// Apply patches
	newContent, _ := dmp.PatchApply(patches, string(fileContent))
	fileConn.Truncate(0)
	_, err = fileConn.WriteAt([]byte(newContent), 0)
	fileConn.Close()

	// Write diff file
	diffFile := base64.RawURLEncoding.EncodeToString([]byte(commit.File))
	diffConn, err := os.Create(REPOPATH + "/.sync/diff/" + diffFile)
	_, err = diffConn.Write([]byte(newContent))
	if err != nil {
		fmt.Println("error writing diff: " + err.Error())
	}
	diffConn.Close()
	return nil
}

func get() {
	index := readLocalIndex()

	// Get commits from remote table
	commits, err := listRepoCommits()
	if err != nil {
		fmt.Println("error getting commits: " + err.Error())
		return
	}

	// TODO: make in O(n)
	for _, commit := range commits {
		applyed := false
		for _, idx := range index {
			if idx.Date == commit.Date {
				applyed = true
			}
		}
		if !applyed {
			err = applyCommit(commit)
		}
	}

	// Write new contents back
	err = writeToLocalIndex(index)
	if err != nil {
		fmt.Println("error writing index: " + err.Error())
	}
}
