package main

import(
	"os"
	"fmt"
	"path/filepath"
)

// Global variables
var REPOPATH string
var SYNCPATH string
var DIFFPATH string

// TODO: Let function receive argument and return a more detailed help
func showHelp() {
	fmt.Println("")
	fmt.Println("Usage: dynasync [command] file ...")
	fmt.Println("")
	fmt.Println("Options for command are:")
	fmt.Println(" init:		create configuration file")
	fmt.Println(" commit")
	fmt.Println(" tag")
	fmt.Println("")
	fmt.Println("Optional parameters for init:")
	fmt.Println(" name:		set the repo name for name")
	fmt.Println("")
	fmt.Println("Needed parameters for commit:")
	fmt.Println(" files:		files to be sent to repository")
	fmt.Println(" -m message:	commit message")
}

func initConfig(args []string) {
	// Create .sync dir
	configDir, err := os.Open(SYNCPATH)
	configDir.Close()

	if err != nil {
		fmt.Println("Creating config dir")
		err = os.Mkdir(SYNCPATH, 0777)

		if err != nil {
			panic("Error creating .sync folder")
		}
	}

	// Create .sync/diff dir
	diffDir, err := os.Open(DIFFPATH)
	diffDir.Close()

	if err != nil {
		fmt.Println("Creating diff dir")
		err = os.Mkdir(DIFFPATH, 0777)

		if err != nil {
			panic("Error creating diff folder")
		}
	}

	// Create .sync/repo.conf file
	configFile, err := os.Open(SYNCPATH + "/repo.conf")
	configFile.Close()

	if err == nil {
		fmt.Println("Config file already exists, nothing to do")
		return
	}

	configFile, err = os.Create(SYNCPATH + "/repo.conf")
	if err != nil {
		panic("Error creating config file")
	}

	if len(args) > 1 {
		_, err = configFile.Write([]byte("name: " + args[1] + "\n"))
	} else {
		_, err = configFile.Write([]byte("name: " + filepath.Base(REPOPATH) + "\n"))
	}

	if err != nil {
		panic("Error writing to config file")
	}

	fmt.Println("Done")
}

func main() {
	// Keep track of the repo path 
	REPOPATH, _ = os.Getwd()
	SYNCPATH = REPOPATH + "/.sync"
	DIFFPATH = SYNCPATH + "/diff"

	if len(os.Args) == 1 {
		fmt.Println("Dynasync v1.0.0: A very simple version control system")
		showHelp()
		return
	}

	for i, arg := range(os.Args[1:]) {
		switch arg {
		case "--help":
			fallthrough
		case "help":
			fallthrough
		case "-h":
			fmt.Println("Dynasync v1.0.0: A very simple version control system")
			showHelp()
			break
		case "init":
			initConfig(os.Args[i + 1:])
			return
		case "commit":
			commit(os.Args[i + 1:])
			return
		default:
			fmt.Println("Error: illegal option", arg)
			showHelp()
		}
	}
}
