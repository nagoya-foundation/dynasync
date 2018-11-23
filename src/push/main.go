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
	fmt.Println(" init:	create configuration file")
	fmt.Println(" commit")
	fmt.Println(" tag")
}

func initConfig(name string) {
	dirPath, _ := os.Getwd()
	dir := filepath.Base(dirPath)
	configFile, err := os.Open(dirPath + "/proj.conf")
	configFile.Close()

	if err == nil {
		fmt.Println("Config file already exists, nothing to do")
		return
	}

	configFile, err = os.Create(dirPath + "/proj.conf")
	if err != nil {
		panic("Error creating config file")
	}

	if name != "" {
		_, err = configFile.Write([]byte("repo: " + name + "\n"))
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
			i++
			if i < argLen {
				initConfig(os.Args[i])
			} else {
				initConfig("")
			}
			break
		default:
			fmt.Println("Error: illegal option", os.Args[i])
			showHelp()
		}
	}
}
