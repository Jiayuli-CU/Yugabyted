package driver

import (
	"bufio"
	"context"
	"cs5424project/transaction/cassandra"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func CqlClient(filepath string, clientNumber int) {
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewReader(file)
	executedTransactions := 0
	var latencies []time.Duration
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
		switch info[0] {
		case "N":
			newOrderParser(ctx, info, buff)
		case "P":
			paymentParser(ctx, info)
		case "D":
			deliveryParser(ctx, info)
		case "O":
			orderStatusParser(ctx, info)
		case "S":
			stockLevelParser(ctx, info)
		case "I":
			popularItemParser(ctx, info)
		case "T":
			topBalanceParser(ctx)
		case "R":
			relatedCustomerParser(ctx, info)

		}
		latencies = append(latencies, time.Since(startTransaction))
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

	fmt.Printf("client %v, total number of transactions processed: %v\n", clientNumber, executedTransactions)
	fmt.Printf("client %v, total execution time: %v\n", clientNumber, executionSeconds)
	//fmt.Printf("client %v, transaction throughput: %v per second\n", clientNumber, executedTransactions/executionSeconds)
	fmt.Printf("client %v, Average transaction latency: %v ms\n", clientNumber, latencyAverage)
	fmt.Printf("client %v, median transaction latency: %v ms\n", clientNumber, latencyMedian)
	fmt.Printf("client %v, 95th percentile transaction latency: %v ms\n", clientNumber, latency95Percent)
	fmt.Printf("client %v, 99th percentile transaction latency: %v ms\n", clientNumber, latency99Percent)
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
