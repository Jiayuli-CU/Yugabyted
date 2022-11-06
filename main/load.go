package main

import (
	"cs5424project/data"
	"cs5424project/store/cassandra"
)

func main() {

	getArgsAndCreateSession()

	defer cassandra.CloseSession()

	data.CqlDataLoader()
}
