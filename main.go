package main

import (
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
	//data.CQLLoadOrder()

	//session := cassandra.GetSession()
	//var num int
	//applied, err := session.Query(`UPDATE cs5424_groupI.districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, 3001, 1, 1, 3002).
	//	ScanCAS(nil, &num)
	//fmt.Println(applied)
	//fmt.Println(num)
	//if !applied {
	//	fmt.Println(err)
	//}
	//if err != nil {
	//	fmt.Println(err)
	//}

}
