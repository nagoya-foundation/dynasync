package main

import (
	"fmt"
	"os"
	"encoding/base64"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func diff(files []string) {
	// Backup each committed file
	for _, file := range(files) {
		fmt.Println("preparing file " + file + " to commit")
		fileInfo, _ := os.Stat(file)
		fileConn, errFile := os.Open(file)

		if errFile != nil {
			panic("error reading file to commit")
		}

		// Copy original file contents to buffer
		content := make([]byte, fileInfo.Size())
		fileConn.Read(content)
		fileConn.Close()

		// Same for diff file
		diffFile := DIFFPATH + base64.RawURLEncoding.EncodeToString([]byte(file))
		diffInfo, errDiff := os.Stat(diffFile)
		var diffContent []byte
		diffConn, err := os.OpenFile(diffFile, os.O_RDWR|os.O_CREATE, 0666)

		if errDiff == nil {
			diffContent = make([]byte, diffInfo.Size())
			diffConn.Read(diffContent)
		} else {
			diffContent = make([]byte, 0)
		}

		// Write new content to diff file
		diffConn.Truncate(0)
		_, err = diffConn.WriteAt(content, 0)
		diffConn.Close()

		// Create diff to send
		dmp := diffmatchpatch.New()
		fmt.Println("creating diff...")
		patches := dmp.PatchMake(string(diffContent), string(content))
		diff := dmp.PatchToText(patches)
		fmt.Println("done")

		// TODO: Send patch to DynamoDB
		fmt.Println(diff)

		if err != nil {
			panic("error writing diff file")
		}
	}
}

// TODO: Add glob support
// TODO: Open an editor to enter message
func commit(args []string) {
	for i := range(args) {
		if string(args[i]) == "-m" {
			if i + 1 < len(args) {
				diff(args[0:i])
				fmt.Println("commited file(s)", args[0:i])
				fmt.Println("with message", args[i + 1])
			} else {
				fmt.Println("error: Missing message parameter")
				showHelp()
			}
			return
		}
	}

	fmt.Println("error: Missing parameters")
	showHelp()

	return
}

