package main

import (
	"cs5424project/driver"
	"cs5424project/store/cassandra"
	"fmt"
	"strconv"
	"sync"
)

func main() {

	defer cassandra.CloseSession()

	var arg1, arg2, arg3, arg4 string
	var n1, n2, n3, n4 int
	var err error
	fmt.Printf("input number: ")
	_, err = fmt.Scanln(&arg1, &arg2, &arg3, &arg4)
	if err != nil {
		fmt.Println("scan user input error")
	}
	n1, err = strconv.Atoi(arg1)
	n2, err = strconv.Atoi(arg2)
	n3, err = strconv.Atoi(arg3)
	n4, err = strconv.Atoi(arg4)
	if err != nil {
		fmt.Printf("input format error: %v\n", err)
	}

	var wg sync.WaitGroup

	for _, i := range []int{n1, n2, n3, n4} {
		wg.Add(1)
		filePath := fmt.Sprintf("data/xact_files/%d.txt", i)
		go driver.CqlClient(&wg, filePath, i)
	}

	wg.Wait()
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
