package main

import (
	"fmt"
)

func commit(args []string) (int) {
	skip := 0
	for i := range(args) {
		if string(args[i][0]) == "-" {
			fmt.Println("commited file(s)", args[:skip])
			break
		}
		skip++
	}

	return skip - 1
}

