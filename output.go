package main

import (
	"cs5424project/output"
	"cs5424project/store/cassandra"
)

func main() {

	defer cassandra.CloseSession()

	output.OutputResult()

}
