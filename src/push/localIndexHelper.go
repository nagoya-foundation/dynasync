package main

import(
	"os"
	"fmt"
)

func readLocalIndex() ([]int64) {
	commits := []int64{}
	var commit int64

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

func writeToLocalIndex(commits []int64) (error) {
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

