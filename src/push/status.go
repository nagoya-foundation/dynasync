package main

import (
	"fmt"
)

// TODO: Open an editor to enter message
func status(args []string) {
	for i := range args {
		if string(args[i]) == "-m" {
			if i+1 < len(args) {
				err := diff(args[0:i], args[i+1])
				if err != nil {
					fmt.Println("error making diff: " + err.Error())
					return
				}
				fmt.Println("commited file(s)", args[0:i])
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
