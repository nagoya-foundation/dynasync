package main

import (
	"fmt"
	"os"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func diff(files []string) {
	// Backup each committed file
	for _, file := range(files) {
		fmt.Println("Preparing file " + file + " to commit")
		diffFile := DIFFPATH + file + ".orig"
		diffInfo, errDiff := os.Stat(diffFile)
		fileInfo, errInfo := os.Stat(file)
		fileConn, errFile := os.Open(file)

		if errInfo != nil || errFile != nil {
			panic("Error reading file to commit")
		}

		content := make([]byte, fileInfo.Size())
		fileConn.Read(content)
		fileConn.Close()

		diffConn, err := os.OpenFile(diffFile, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			panic("Error creating diff file")
		}

		// First time committing this file?
		if errDiff == nil {
			// No, read content and create a patch
			diffContent := make([]byte, diffInfo.Size())
			diffConn.Read(diffContent)

			dmp := diffmatchpatch.New()
			patches := dmp.PatchMake(string(diffContent), string(content))
			diff := dmp.PatchToText(patches)
			fmt.Println(diff)

			diffConn.Truncate(0)

		}

		_, err = diffConn.WriteAt(content, 0)
		diffConn.Close()

		if err != nil {
			panic("Error writing diff file")
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

