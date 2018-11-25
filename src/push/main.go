package main

import(
	"os"
	"fmt"
	"path/filepath"
)

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
	fmt.Println(" file:		file to be sent to repository")
	fmt.Println(" -m message:	commit message")
}

func initConfig(args []string) {
	dirPath, _ := os.Getwd()
	dir := filepath.Base(dirPath)
	configFile, err := os.Open(dirPath + "/.sync.conf")
	configFile.Close()

	if err == nil {
		fmt.Println("Config file already exists, nothing to do")
		return
	}

	configFile, err = os.Create(dirPath + "/.sync.conf")
	if err != nil {
		panic("Error creating config file")
	}

	if len(args) > 1 {
		_, err = configFile.Write([]byte("repo: " + args[1] + "\n"))
	} else {
		_, err = configFile.Write([]byte("repo: " + dir + "\n"))
	}

	if err != nil {
		panic("Error writing to config file")
	}
}

func main() {

	argLen := len(os.Args)
	if argLen == 1 {
		fmt.Println("Dynasync v1.0.0: A very simple version control system")
		showHelp()
		return
	}

	for i := 1; i < argLen; i++ {
		switch os.Args[i] {
		case "--help":
			fallthrough
		case "help":
			fallthrough
		case "-h":
			fmt.Println("Dynasync v1.0.0: A very simple version control system")
			showHelp()
			break
		case "init":
			initConfig(os.Args[i:])
			return
		case "commit":
			commit(os.Args[i:])
			return
		default:
			fmt.Println("Error: illegal option", os.Args[i])
			showHelp()
		}
	}
}
