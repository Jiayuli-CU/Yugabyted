package main

import (
	"cs5424project/store/cassandra"
	"fmt"
)

func main() {
	//db := postgre.GetDB()
	session := cassandra.GetSession()
	fmt.Println(session)
}
