package main

import(
	"os"
	"fmt"
)

func readLocalIndex() ([]int64) {
	indexFile, _ := os.Open(REPOPATH + "/.sync/index")
	commits := []int64{}
	var commit int64
	for _, err := fmt.Fscanf(indexFile, "%d", &commit); err == nil; {
		commits = append(commits, commit)
	}

	indexFile.Close()
	return commits
}

func writeToLocalIndex(commits []int64) (error) {
	// Recreate index file
	indexFile, _ := os.Create(REPOPATH + "/.sync/index")
	for _, commit := range commits {
		_, err := fmt.Fprint(indexFile, commit)
		if err != nil {
			return err
		}
	}
	indexFile.Close()

	return nil
}

