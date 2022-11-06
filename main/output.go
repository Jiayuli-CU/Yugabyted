package main

import (
	"cs5424project/output"
	"cs5424project/store/cassandra"
)

func main() {

	getArgsAndCreateSession()

	defer cassandra.CloseSession()

	output.OutputResult()
	
}
