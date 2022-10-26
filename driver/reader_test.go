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
	SqlClient(filePath, 0)
}

func TestPayment(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_payment.txt"
	SqlClient(filePath, 0)
}

func TestDelivery(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_delivery.txt"
	SqlClient(filePath, 0)
}

func TestOrderStatus(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_order_status.txt"
	SqlClient(filePath, 0)
}
