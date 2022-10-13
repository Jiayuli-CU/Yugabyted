package main

import (
	"cs5424project/data"
	"fmt"
)

func main() {
	err := data.LoadWarehouse()
	if err != nil {
		fmt.Println(err.Error())
	}
}
