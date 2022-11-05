package main

import (
	"cs5424project/driver"
	"fmt"
	"os"
	"strconv"
	"sync"
)

func main() {

	args := os.Args
	var n1, n2, n3, n4 int
	var err error
	n1, err = strconv.Atoi(args[0])
	n2, err = strconv.Atoi(args[1])
	n3, err = strconv.Atoi(args[2])
	n4, err = strconv.Atoi(args[3])
	if err != nil {
		fmt.Printf("input format error: %v\n", err)
	}

	var wg sync.WaitGroup

	for _, i := range []int{n1, n2, n3, n4} {
		wg.Add(1)
		filePath := fmt.Sprintf("data/xact_files/%d.txt", i)
		go driver.SqlClient(&wg, filePath, i)
	}

	wg.Wait()
}
