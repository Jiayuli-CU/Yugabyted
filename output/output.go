package output

import (
	"cs5424project/store/cassandra"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

var session = cassandra.GetSession()

func OutputResult() {

	outputFile("clients")

	var err error
	var (
		warehouseYearToDateAmountInt int
		districtYearToDateAmountInt  int
		districtNextOrderSum         int
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
		OrderLineQuantitySum      int
		DistrictNextOrderSum      int
	}

	err = session.Query(`select sum(warehouse_year_to_date_payment) from cs5424_groupi.warehouse_counter`).Scan(&warehouseYearToDateAmountInt)
	if err != nil {
		fmt.Println("find warehouse year to date payment error: ", err)
	}
	err = session.Query(`select sum(district_year_to_date_payment) from cs5424_groupi.district_counter`).Scan(&districtYearToDateAmountInt)
	if err != nil {
		fmt.Println("find district year to date payment error: ", err)
	}
	err = session.Query(`select sum(next_order_number) from cs5424_groupi.districts`).Scan(&districtNextOrderSum)
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

	orderLineQuantitySum := 0
	//var orderLine cassandra.OrderLine
	var orderLines []cassandra.OrderLine
	scanner := session.Query(`select order_lines from cs5424_groupi.orders`).Iter().Scanner()
	for scanner.Next() {
		scanner.Scan(&orderLines)
		for _, ol := range orderLines {
			orderLineQuantitySum += ol.Quantity
		}

	}

	outputTitle := []string{
		"sum(W_YTD)",
		"sum(D_YTD)",
		"sum(D_NEXT_O_ID)",
		"sum(C_BALANCE)",
		"sum(C_YTD_PAYMENT)",
		"sum(C_PAYMENT_CNT)",
		"sum(DELIVERY_CNT)",
		"max(O_ID)",
		"sum(O_OL_CNT)",
		"sum(OL_AMOUNT)",
		"sum(OL_QUANTITY)",
		"sum(S_QUANTITY)",
		"sum(S_YTD)",
		"sum(S_ORDER_CNT)",
		"sum(S_REMOTE_CNT)",
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
		OrderLineQuantitySum:      orderLineQuantitySum,
		DistrictNextOrderSum:      districtNextOrderSum,
	}

	outputData := []string{
		output.WarehouseYearToDateAmount,
		output.DistrictYearToDateAmount,
		strconv.Itoa(districtNextOrderSum),
		output.Balance,
		output.CustomerYearToDateAmount,
		output.CustomerPayment,
		strconv.Itoa(output.Delivery),
		strconv.Itoa(maxOrderId),
		strconv.Itoa(orderLineNumber),
		output.OrderLineAmount,
		strconv.Itoa(orderLineQuantitySum),
		strconv.Itoa(stockQuantity),
		strconv.Itoa(stockYTDQuantity),
		strconv.Itoa(orderCount),
		strconv.Itoa(remoteCount),
	}

	dbstateOutput := [][]string{
		outputTitle,
		outputData,
	}

	CsvWriter("dbstate", dbstateOutput)
	fmt.Printf("%+v\n", output)

}

func outputFile(fileName string) {

	var output [][]string
	throughput := make([]int, 20)
	output = append(output, []string{
		//"ClientNumber",
		"executedTransactions",
		"executionSeconds",
		"throughput",
		"latencyAverage",
		"latencyMedian",
		"latency95Percent",
		"latency99Percent",
	})
	for i := 0; i < 20; i++ {
		file := fmt.Sprintf("client_output%v", i)
		content := CsvReader(file)
		output = append(output, content)
		throughput[i], _ = strconv.Atoi(content[2])
	}

	sort.Slice(throughput, func(i, j int) bool {
		return throughput[i] < throughput[j]
	})

	throughputSum := 0
	for _, t := range throughput {
		throughputSum += t
	}

	throughputTitle := []string{
		"min_throughput",
		"max_throughput",
		"avg_throughput",
	}
	throughputData := []string{
		fmt.Sprintf("%v", throughput[0]),
		fmt.Sprintf("%v", throughput[19]),
		fmt.Sprintf("%.2f", float32(throughputSum)/20),
	}
	throughputOutput := [][]string{
		throughputTitle,
		throughputData,
	}
	CsvWriter("throughput", throughputOutput)
	CsvWriter(fileName, output)
}

func CsvWriter(file string, data [][]string) {
	csvFile, err := os.Create(file)
	if err != nil {
		log.Println("fail to open file")
	}
	defer csvFile.Close()

	w := csv.NewWriter(csvFile)
	w.WriteAll(data)
	w.Flush()
}

func CsvReader(file string) []string {
	fs, err := os.Open(file)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer fs.Close()

	r := csv.NewReader(fs)
	content, err := r.ReadAll()
	return content[0]
}
