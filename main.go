package main

import (
	"cs5424project/driver"
	"cs5424project/store/cassandra"
)

func main() {

	defer cassandra.CloseSession()

	//data.CqlDataLoader()
	//session := cassandra.GetSession()

	//cassandra.QueryTest()

	//var w []int
	//w = append(w, 1)
	//err := cassandra2.NewOrder(1, 1, 1, 1, w, w, w)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//data.CqlDataLoader()
	//
	//TestTopBalanceTransaction()
	//TestOrderStatustransaction()
	//TestDeliveryTransaction()
	//TestNewOrderTransaction()
	//TestPopularItemTransaction()
	//TestRelatedCustomerTransaction()
	TestStockLevelTransacction()
	//TestAllTransactions()
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

func TestAllTransactions() {
	var filePath string
	//for i := 0; i < 20; i++ {
	//	filePath = fmt.Sprintf("data/xact_files/%v.txt", i)
	//}
	filePath = "data/xact_files/0.txt"
	driver.CqlClient(filePath, 0)
}
