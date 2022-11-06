package main

import (
	"fmt"
	"os"
)

func main() {

	args := os.Args[1:]
	ips := args[:5]
	username := args[5]
	password := args[6]

	fmt.Println(ips, username, password)

}
