package main

import (
	"cs5424project/data"
	"cs5424project/store/cassandra"
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
	//cassandra.QueryTest()

	//var w []int
	//w = append(w, 1)
	//err := cassandra2.NewOrder(1, 1, 1, 1, w, w, w)
	//if err != nil {
	//	fmt.Println(err)
	//}

	//cassandra2.PaymentTransaction(1, 1, 1, 23.0)
	//data.CQLLoadOrder()
	//cassandra2.OrderStatusTransaction(1, 1, 1)
	data.CQLLoadOrder()

}
