package output

import (
	"cs5424project/store/cassandra"
	"fmt"
)

var session = cassandra.GetSession()

func OutputResult() {

	var err error
	var (
		warehouseYearToDateAmountInt int
		districtYearToDateAmountInt  int
		balanceInt                   int
		customerYearToDateAmountInt  int
		customerPaymentInt           int
		delivery                     int
		maxOrderId                   int
		orderLineNumber              int
		orderLineAmountInt           int
		stockQuantity                int
		stockYTDQuantity             int
		orderCount                   int
		remoteCount                  int
	)

	type Output struct {
		WarehouseYearToDateAmount string
		DistrictYearToDateAmount  string
		Balance                   string
		CustomerYearToDateAmount  string
		CustomerPayment           string
		Delivery                  int
		MaxOrderId                int
		OrderLineNumber           int
		OrderLineAmount           string
		StockQuantity             int
		StockYTDQuantity          int
		OrderCount                int
		RemoteCount               int
	}

	err = session.Query(`select sum(warehouse_year_to_date_payment) from cs5424_groupi.warehouse_counter`).Scan(&warehouseYearToDateAmountInt)
	if err != nil {
		fmt.Println("find warehouse year to date payment error: ", err)
	}
	err = session.Query(`select sum(district_year_to_date_payment) from cs5424_groupi.district_counter`).Scan(&districtYearToDateAmountInt)
	if err != nil {
		fmt.Println("find district year to date payment error: ", err)
	}
	err = session.Query(`select sum(balance), sum(year_to_date_payment), sum(payment_count), sum(delivery_count) from cs5424_groupi.customer_counters`).
		Scan(&balanceInt, &customerYearToDateAmountInt, &customerPaymentInt, &delivery)
	if err != nil {
		fmt.Println("find customer error: ", err)
	}
	err = session.Query(`select max(order_id), sum(items_number), sum(total_amount) from cs5424_groupi.orders`).
		Scan(&maxOrderId, &orderLineNumber, &orderLineAmountInt)
	if err != nil {
		fmt.Println("find orders error: ", err)
	}
	err = session.Query(`select sum(quantity), sum(total_quantity), sum(order_count), sum(remote_count) from cs5424_groupi.stock_counters`).
		Scan(&stockQuantity, &stockYTDQuantity, &orderCount, &remoteCount)
	if err != nil {
		fmt.Println("find stocks error: ", err)
	}

	output := Output{
		WarehouseYearToDateAmount: fmt.Sprintf("%.2f", float64(warehouseYearToDateAmountInt)/100),
		DistrictYearToDateAmount:  fmt.Sprintf("%.2f", float64(districtYearToDateAmountInt)/100),
		Balance:                   fmt.Sprintf("%.2f", float64(balanceInt)/100),
		CustomerPayment:           fmt.Sprintf("%.2f", float64(customerPaymentInt)/100),
		CustomerYearToDateAmount:  fmt.Sprintf("%.2f", float64(customerYearToDateAmountInt)/100),
		Delivery:                  delivery,
		MaxOrderId:                maxOrderId,
		OrderCount:                orderCount,
		RemoteCount:               remoteCount,
		OrderLineNumber:           orderLineNumber,
		OrderLineAmount:           fmt.Sprintf("%.2f", float64(orderLineAmountInt)/100),
		StockQuantity:             stockQuantity,
		StockYTDQuantity:          stockYTDQuantity,
	}

	fmt.Printf("%+v\n", output)

}
