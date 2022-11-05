package driver

import (
	"fmt"
	"strconv"
	"testing"
)

//func TestSqlClient(t *testing.T) {
//	filePath := "../data/test_xact_files/test.txt"
//	SqlClient(filePath, 0)
//}

func TestSqlClient(t *testing.T) {
	s := "5"
	i, _ := strconv.Atoi(s)
	fmt.Println(s, i)
}

func TestNewOrder(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_new_order.txt"
	SqlClient(nil, filePath, 0)
}

func TestPayment(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_payment.txt"
	SqlClient(nil, filePath, 0)
}

func TestDelivery(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_delivery.txt"
	SqlClient(nil, filePath, 0)
}

func TestOrderStatus(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_order_status.txt"
	SqlClient(nil, filePath, 0)
}

func TestStockLevel(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_stock_level.txt"
	SqlClient(nil, filePath, 0)
}

func TestPopularItem(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_popular_item.txt"
	SqlClient(nil, filePath, 0)
}

func TestTopBalance(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_top_balance.txt"
	SqlClient(nil, filePath, 0)
}

func TestRelatedCustomer(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_related_customer.txt"
	SqlClient(nil, filePath, 0)
}
