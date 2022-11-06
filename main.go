package main

import (
	"cs5424project/runtime"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	args := os.Args
	if strings.Compare(args[1], "-cf") == 0 {
		fmt.Println("Setting up db connection configuration...")
		runtime.Config(args[2], args[3], args[4], args[5], args[6])
	} else if strings.Compare(args[1], "-ld") == 0 {
		fmt.Println("Loading all data to database...")
		runtime.LoadDataToDB()
	} else if strings.Compare(args[1], "-xs") == 0 {
		fmt.Println("Starting transactions...")
		n1, _ := strconv.Atoi(args[2])
		n2, _ := strconv.Atoi(args[3])
		n3, _ := strconv.Atoi(args[4])
		n4, _ := strconv.Atoi(args[5])
		runtime.StartTransactions(n1, n2, n3, n4)
	} else {
		fmt.Println("Wrong function input type! Should be in [-cf, -ld, -xs]")
	}
}
