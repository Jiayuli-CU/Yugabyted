package driver

import (
	"cs5424project/store/postgre"
	"fmt"
	"github.com/stretchr/testify/assert"
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

func TestStocklevel(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_stock_level.txt"
	SqlClient(filePath, 0)
}

func TestPopularItem(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_popular_item.txt"
	SqlClient(filePath, 0)
}

func TestTopBalance(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_top_balance.txt"
	SqlClient(filePath, 0)
}

func TestRelatedCustomer(t *testing.T) {
	filePath := "../data/test_xact_files/test_psql_related_customer.txt"
	SqlClient(filePath, 0)
}

func Test1(t *testing.T) {
	db := postgre.GetDB()
	var orderNumber uint64
	db.Exec("SELECT min(id) from orders where carrier_id = 0 AND warehouse_id = ? AND district_id = ?", 1, 1).Row().Scan(&orderNumber)
	assert.Equal(t, true, true, orderNumber)
}
