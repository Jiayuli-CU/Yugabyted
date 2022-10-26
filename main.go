package main

import (
	"cs5424project/store/cassandra"
	"fmt"
)

func main() {

	//var wg sync.WaitGroup
	//
	//for i := 0; i < 3; i++ {
	//	filepath := fmt.Sprintf("data/xact_files/%v.txt", i)
	//	wg.Add(1)
	//	go func(filepath string, clientNumber int) {
	//		defer wg.Done()
	//		driver.SqlClient(filepath, clientNumber)
	//	}(filepath, i)
	//}
	//
	//wg.Wait()
	//fmt.Println("main exit")
	defer cassandra.CloseSession()
	err := cassandra.QueryTest()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("good")
	}

}
