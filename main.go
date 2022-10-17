package main

import (
	"cs5424project/store/cassandra"
	"fmt"
)

func main() {
	session := cassandra.GetSession()
	fmt.Println(session)
}
