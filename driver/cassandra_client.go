package driver

import (
	"bufio"
	"context"
	output2 "cs5424project/output"
	"cs5424project/transaction/cassandra"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func CqlClient(wg *sync.WaitGroup, filepath string, clientNumber int) {
	defer wg.Done()
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewReader(file)
	executedTransactions := 0
	var latencies []time.Duration
	var newOrderLatencies []time.Duration
	var paymentLatencies []time.Duration
	var deliveryLatencies []time.Duration
	var orderStatusLatencies []time.Duration
	var stockLevelLatencies []time.Duration
	var topBalanceLatencies []time.Duration
	var relatedCustomerLatencies []time.Duration
	var popularItemLatencies []time.Duration
	ctx := context.Background()
	start := time.Now()
	for {
		line, err := buff.ReadString('\n')
		if err == io.EOF {
			break
		}
		executedTransactions += 1
		line = strings.Replace(line, "\n", "", -1)
		info := strings.Split(line, ",")
		startTransaction := time.Now()
		var latency time.Duration
		switch info[0] {
		case "N":
			newOrderParser(ctx, info, buff)
			latency = time.Since(startTransaction)
			newOrderLatencies = append(newOrderLatencies, latency)
		case "P":
			paymentParser(ctx, info)
			latency = time.Since(startTransaction)
			paymentLatencies = append(paymentLatencies, latency)
		case "D":
			deliveryParser(ctx, info)
			latency = time.Since(startTransaction)
			deliveryLatencies = append(deliveryLatencies, latency)
		case "O":
			orderStatusParser(ctx, info)
			latency = time.Since(startTransaction)
			orderStatusLatencies = append(orderStatusLatencies, latency)
		case "S":
			stockLevelParser(ctx, info)
			latency = time.Since(startTransaction)
			stockLevelLatencies = append(stockLevelLatencies, latency)
		case "I":
			popularItemParser(ctx, info)
			latency = time.Since(startTransaction)
			popularItemLatencies = append(popularItemLatencies, latency)
		case "T":
			topBalanceParser(ctx)
			latency = time.Since(startTransaction)
			topBalanceLatencies = append(topBalanceLatencies, latency)
		case "R":
			relatedCustomerParser(ctx, info)
			latency = time.Since(startTransaction)
			relatedCustomerLatencies = append(relatedCustomerLatencies, latency)
		}
		latencies = append(latencies, latency)
	}
	totalExecutionTime := time.Since(start)
	executionSeconds := int(totalExecutionTime.Seconds())
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	var sumLatency int64
	for _, t := range latencies {
		sumLatency += t.Milliseconds()
	}
	latencyAverage := sumLatency / int64(executedTransactions)
	latencyMedian := latencies[executedTransactions/2].Milliseconds()
	latency95Percent := latencies[int(float32(executedTransactions)*0.95)].Milliseconds()
	latency99Percent := latencies[int(float32(executedTransactions)*0.99)].Milliseconds()

	output := []string{
		fmt.Sprintf("%v", executedTransactions),
		fmt.Sprintf("%v", executionSeconds),
		fmt.Sprintf("%v", executedTransactions/executionSeconds),
		fmt.Sprintf("%v", latencyAverage),
		fmt.Sprintf("%v", latencyMedian),
		fmt.Sprintf("%v", latency95Percent),
		fmt.Sprintf("%v", latency99Percent),
	}

	output2.CsvWriter(fmt.Sprintf("output_files/client_output%v", clientNumber), [][]string{output})

	var latencyInfo [][]string
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(newOrderLatencies, "New-Order"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(paymentLatencies, "Payment"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(deliveryLatencies, "Delivery"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(orderStatusLatencies, "Order-Status"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(stockLevelLatencies, "Stock-Level"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(popularItemLatencies, "Popular-Item"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(topBalanceLatencies, "Top-Balance"))
	latencyInfo = append(latencyInfo, generateLatencyAnalysis(relatedCustomerLatencies, "Related-Customer"))

	output2.CsvWriter(fmt.Sprintf("output_files/client%v_transaction_info", clientNumber), latencyInfo)

	fmt.Printf("client %v, total number of transactions processed: %v\n", clientNumber, executedTransactions)
	fmt.Printf("client %v, total execution time: %v s\n", clientNumber, executionSeconds)
	fmt.Printf("client %v, transaction throughput: %v per second\n", clientNumber, executedTransactions/executionSeconds)
	fmt.Printf("client %v, Average transaction latency: %v ms\n", clientNumber, latencyAverage)
	fmt.Printf("client %v, median transaction latency: %v ms\n", clientNumber, latencyMedian)
	fmt.Printf("client %v, 95th percentile transaction latency: %v ms\n", clientNumber, latency95Percent)
	fmt.Printf("client %v, 99th percentile transaction latency: %v ms\n", clientNumber, latency99Percent)

}

func generateLatencyAnalysis(latencies []time.Duration, transaction string) []string {
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	var sumLatency int64
	for _, t := range latencies {
		sumLatency += t.Milliseconds()
	}
	total := len(latencies)
	latencyAverage := sumLatency / int64(total)
	latencyMedian := latencies[total/2].Milliseconds()
	latency95Percent := latencies[int(float32(total)*0.95)].Milliseconds()
	latency99Percent := latencies[int(float32(total)*0.99)].Milliseconds()
	output := []string{
		transaction,
		strconv.Itoa(total),
		fmt.Sprintf("%v", latencyAverage),
		fmt.Sprintf("%v", latencyMedian),
		fmt.Sprintf("%v", latency95Percent),
		fmt.Sprintf("%v", latency99Percent),
	}
	return output
}

func newOrderParser(ctx context.Context, info []string, buff *bufio.Reader) {
	customerId, _ := strconv.Atoi(info[1])
	warehouseId, _ := strconv.Atoi(info[2])
	districtId, _ := strconv.Atoi(info[3])
	total, _ := strconv.Atoi(info[4])
	itemNumbers := make([]int, total)
	supplierWarehouses := make([]int, total)
	quantities := make([]int, total)
	for i := 0; i < total; i++ {
		subLine, _ := buff.ReadString('\n')
		subInfo := strings.Split(strings.Replace(subLine, "\n", "", -1), ",")
		itemNumber, _ := strconv.Atoi(subInfo[0])
		supplyWarehouseId, _ := strconv.Atoi(subInfo[1])
		quantity, _ := strconv.Atoi(subInfo[2])
		itemNumbers[i] = itemNumber
		supplierWarehouses[i] = supplyWarehouseId
		quantities[i] = quantity
	}

	err := cassandra.NewOrder(ctx, warehouseId, districtId, customerId, total, itemNumbers, supplierWarehouses, quantities)
	if err != nil {
		fmt.Printf("New Order Transaction failed: %s\n", err.Error())
	}

}

func paymentParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	districtId, _ := strconv.Atoi(info[2])
	customerId, _ := strconv.Atoi(info[3])
	payment, _ := strconv.ParseFloat(info[4], 32)
	err := cassandra.PaymentTransaction(ctx, warehouseId, districtId, customerId, float32(payment))
	if err != nil {
		fmt.Printf("Payment Transaction failed: %s\n", err.Error())
	}
}

func deliveryParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	carrierId, _ := strconv.Atoi(info[2])
	err := cassandra.DeliveryTransaction(ctx, warehouseId, carrierId)
	if err != nil {
		fmt.Printf("Delivery Transaction failed: %s\n", err.Error())
	}
}

func orderStatusParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	districtId, _ := strconv.Atoi(info[2])
	customerId, _ := strconv.Atoi(info[3])
	err := cassandra.OrderStatusTransaction(ctx, warehouseId, districtId, customerId)
	if err != nil {
		fmt.Printf("Order-Status Transaction failed: %s\n", err.Error())
	}
}

func stockLevelParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	districtId, _ := strconv.Atoi(info[2])
	threshold, _ := strconv.Atoi(info[3])
	orderNumber, _ := strconv.Atoi(info[4])
	err := cassandra.StockLevelTransaction(ctx, warehouseId, districtId, threshold, orderNumber)
	if err != nil {
		fmt.Printf("Stock-Level Transaction failed: %s\n", err.Error())
	}
}

func popularItemParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	districtId, _ := strconv.Atoi(info[2])
	orderNumber, _ := strconv.Atoi(info[3])
	err := cassandra.PopularItemTransaction(ctx, warehouseId, districtId, orderNumber)
	if err != nil {
		fmt.Printf("Popular-Item Transaction failed: %s\n", err.Error())
	}
}

func topBalanceParser(ctx context.Context) {
	err := cassandra.TopBalanceTransaction(ctx)
	if err != nil {
		fmt.Printf("Top-Balance Transaction failed: %s\n", err.Error())
	}
}

func relatedCustomerParser(ctx context.Context, info []string) {
	warehouseId, _ := strconv.Atoi(info[1])
	districtId, _ := strconv.Atoi(info[2])
	customerId, _ := strconv.Atoi(info[3])
	err := cassandra.RelatedCustomerTransaction(ctx, warehouseId, districtId, customerId)
	if err != nil {
		fmt.Printf("Related-Customer Transaction failed: %s\n", err.Error())
	}
}
