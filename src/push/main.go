package main

import(
	"os"
	"fmt"
	"path/filepath"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Global variables
var REPOPATH string
var SYNCPATH string
var DIFFPATH string
var REPONAME string
var DYNAMODB *dynamodb.DynamoDB

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

// TODO: Read config file and import settings
func findConfig() {
	// Try to find the config folder in parent folders
	for REPOPATH != "/" {
		_, err := os.Stat(REPOPATH + "/.sync/")
		if err == nil {
			SYNCPATH = REPOPATH + "/.sync/"
			DIFFPATH = SYNCPATH + "diff/"
			return
		}
		REPOPATH = filepath.Dir(REPOPATH)
	}

	panic("diff dir not found, make sure you ran init first")
}

// TODO: Take --aws-profile and --aws-region as argument and save in config
// file
// FIXME: Let init again with another name, now it creates only the new
// remote table
func initConfig(args []string) {

	if len(args) > 1 {
		REPONAME = "repo-" + args[1]
	} else {
		REPONAME = "repo-" + filepath.Base(REPOPATH)
	}

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
	configFile, err := os.Open(SYNCPATH + "repo.conf")
	configFile.Close()

	if err == nil {
		fmt.Println("Config file already exists")
	} else {
		configFile, err = os.Create(SYNCPATH + "repo.conf")
		if err != nil {
			panic("Error creating config file")
		}

		_, err = configFile.Write([]byte("name: " + REPONAME + "\n"))
		if err != nil {
			panic("Error writing to config file")
		}
	}

	// Create DynamoDB client
	hasRepo, err := checkRepoExistence(REPONAME)
	if err != nil {
		panic("Error checking for repo existence: " + err.Error())
	} else if !hasRepo {
		err = createRepo(REPONAME)
		if err != nil {
			panic("Error creating remote repo: " + err.Error())
		}
	} else {
		fmt.Println("Remote repo found")
	}

	fmt.Println("Done")
}

func main() {
	// Keep track of the repo path
	REPOPATH, _ = os.Getwd()
	SYNCPATH = REPOPATH + "/.sync/"
	DIFFPATH = SYNCPATH + "diff/"
	DYNAMODB = startDynamoDBSession("blmayer", "sa-east-1")

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
			findConfig()
			commit(os.Args[i + 1:])
			return
		default:
			fmt.Println("error: illegal option", arg)
			showHelp()
		}
	}
}

