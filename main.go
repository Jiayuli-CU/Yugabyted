package main

import (
	"cs5424project/driver"
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

	//data.CqlDataLoader()
	session := cassandra.GetSession()

	var nextOrderNumber int

	err := session.Query(`SELECT COUNT(*) FROM cs5424_groupi.districts`).Scan(&nextOrderNumber)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(nextOrderNumber)
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

	//TestTopBalanceTransaction()
	//TestOrderStatustransaction()
	//TestDeliveryTransaction()
	//TestNewOrderTransaction()
	//TestPopularItemTransaction()
	//TestRelatedCustomerTransaction()
	//TestStockLevelTransacction()
}

func TestTopBalanceTransaction() {
	filePath := "data/test_xact_files/test_top_balance.txt"
	driver.CqlClient(filePath, 0)
}

func TestOrderStatusTransaction() {
	filePath := "data/test_xact_files/test_order_status.txt"
	driver.CqlClient(filePath, 0)
}

func TestDeliveryTransaction() {
	filePath := "data/test_xact_files/test_delivery.txt"
	driver.CqlClient(filePath, 0)
}

func TestNewOrderTransaction() {
	filePath := "data/test_xact_files/test_new_order.txt"
	driver.CqlClient(filePath, 0)
}

func TestPaymentTransaction() {
	filePath := "data/test_xact_files/test_payment.txt"
	driver.CqlClient(filePath, 0)
}

func TestPopularItemTransaction() {
	filePath := "data/test_xact_files/test_popular_item.txt"
	driver.CqlClient(filePath, 0)
}

func TestRelatedCustomerTransaction() {
	filePath := "data/test_xact_files/test_related_customer.txt"
	driver.CqlClient(filePath, 0)
}

func TestStockLevelTransacction() {
	filePath := "data/test_xact_files/test_stock_level.txt"
	driver.CqlClient(filePath, 0)
}
