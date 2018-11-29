package main

import (
	"fmt"
	"os"
	//"github.com/sergi/go-diff/diffmatchpatch"
)

func diff(files []string) {
	// Backup each committed file
	for _, file := range(files) {
		fmt.Println("Preparing file " + file + " to commit")
		_, err := os.Stat(DIFFPATH + "/file-" + file + ".diff")

		// First time committing this file?
		if err != nil {
			// Yes, backup the whole file
			origInfo, err := os.Stat(file)
			origFile, err := os.Open(file)
			content := make([]byte, origInfo.Size())
			_, err = origFile.Read(content)

			if err != nil {
				panic("Error reading file to commit")
			}

			diffFile, err := os.Create(DIFFPATH + "/file-" + file + ".orig")
			_, err = diffFile.Write(content)

			if err != nil {
				panic("Error copying file to diff folder")
			}
			fmt.Println("File backed up")
		}
	}
}

func commit(args []string) {
	_, err := os.Stat(DIFFPATH)
	if err != nil {
		fmt.Println("diff dir not found, make sure you run init first")
		return
	}

	for i := range(args) {
		if string(args[i]) == "-m" {
			if i + 1 < len(args) {
				diff(args[1:i])
				fmt.Println("commited file(s)", args[1:i])
				fmt.Println("with message", args[i + 1])
			} else {
				fmt.Println("Error: Missing message parameter")
				showHelp()
			}
			return
		}
	}

	fmt.Println("Error: Missing parameters")
	showHelp()

	return
}
