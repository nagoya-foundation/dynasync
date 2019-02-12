package main

import (
	"fmt"
	"os"
)

func readLocalIndex() []Commit {
	commits := []Commit{}
	var commit Commit

	indexFile, err := os.Open(REPOPATH + "/.sync/index")
	if err != nil {
		return commits
	}

	for {
		if _, err = fmt.Fscan(indexFile, &commit); err != nil {
			break
		}

		commits = append(commits, commit)
	}

	indexFile.Close()
	return commits
}

func writeToLocalIndex(commits []Commit) error {
	// Recreate index file
	indexFile, _ := os.Create(REPOPATH + "/.sync/index")
	for _, commit := range commits {
		_, err := fmt.Fprintln(indexFile, commit)
		if err != nil {
			return err
		}
	}
	indexFile.Close()

	return nil
}
