package main

import (
	"fmt"
)

func commit(args []string) {
	for i := range(args) {
		if string(args[i]) == "-m" {
			if i + 1 < len(args) {
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

