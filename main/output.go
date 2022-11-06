package main

import (
	"cs5424project/output"
	"cs5424project/store/cassandra"
	"os"
)

func main() {

	getArgsAndCreateSession()

	defer cassandra.CloseSession()

	output.OutputResult()

}

func getArgsAndCreateSession() {
	args := os.Args[1:]
	ips := args[:5]
	username := args[5]
	password := args[6]
	cassandra.CreateSession(ips, username, password)
}
