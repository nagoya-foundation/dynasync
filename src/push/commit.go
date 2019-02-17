package main

import (
	"encoding/base64"
	"fmt"
	"github.com/sergi/go-diff/diffmatchpatch"
	"os"
	"path/filepath"
	"time"
)

func diff(files []string, mess string) error {
	// Backup each committed file
	thisDir, _ := os.Getwd()
	os.Chdir(REPOPATH)
	for _, file := range files {
		// TODO: Make path system independent
		file, pathErr := filepath.Rel(REPOPATH, thisDir+"/"+file)
		if pathErr != nil {
			fmt.Println("relative path error: " + pathErr.Error())
			continue
		}

		fileInfo, _ := os.Stat(file)
		fileConn, errFile := os.Open(file)

		if errFile != nil {
			panic("error reading file to commit: " + errFile.Error())
		}

		// Copy original file contents to buffer
		content := make([]byte, fileInfo.Size())
		fileConn.Read(content)
		fileConn.Close()

		// Same for diff file
		diffFile := REPOPATH + "/.sync/diff/" +
			base64.RawURLEncoding.EncodeToString([]byte(file))
		diffInfo, errDiff := os.Stat(diffFile)
		var diffContent []byte
		diffConn, _ := os.OpenFile(diffFile, os.O_RDWR|os.O_CREATE, 0666)

		if errDiff == nil {
			diffContent = make([]byte, diffInfo.Size())
			diffConn.Read(diffContent)
		} else {
			diffContent = make([]byte, 0)
		}

		// Write new content to diff file
		diffConn.Truncate(0)
		_, err := diffConn.WriteAt(content, 0)
		diffConn.Close()

		if err != nil {
			panic("error writing diff file: " + err.Error())
		}

		// Create diff to send
		dmp := diffmatchpatch.New()
		fmt.Println("creating diff...")
		patches := dmp.PatchMake(string(diffContent), string(content))
		diff := dmp.PatchToText(patches)

		// Build Commit struct
		commitData := Commit{
			Repo:    REPONAME,
			File:    file,
			Date:    time.Now().Unix(),
			Diff:    diff,
			Message: mess,
		}

		// Send patch to DynamoDB
		err = sendCommit(commitData)
		if err != nil {
			fmt.Println("commit error: " + err.Error())
			return err
		}

		// Update index table
		err = updateIndex(commitData)
		if err != nil {
			fmt.Println("index update error: " + err.Error())
			return err
		}

		// Write commit time to index file
		index := readLocalIndex()

		// Write new contents back
		newIndex := append(index, commitData)
		err = writeToLocalIndex(newIndex)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: Open an editor to enter message
func commit(args []string) {
	for i := range args {
		if string(args[i]) == "-m" {
			if i+1 < len(args) {
				err := diff(args[0:i], args[i+1])
				if err != nil {
					fmt.Println("error making diff: " + err.Error())
					return
				}
				fmt.Println("committed file(s)", args[0:i])
				fmt.Println("with message", args[i+1])
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
