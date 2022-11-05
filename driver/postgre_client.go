package driver

import (
	"bufio"
	"cs5424project/transaction/postgre"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func SqlClient(wg *sync.WaitGroup, filepath string, clientNumber int) {
	if wg != nil {
		defer wg.Done()
	}
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewReader(file)
	executedTransactions := 0
	var latencies []time.Duration
	start := time.Now()
	var info []string
	runningCount := 0
	for {
		line, err := buff.ReadString('\n')
		if err == io.EOF {
			break
		}
		executedTransactions += 1
		info = strings.Split(strings.Replace(line, "\n", "", -1), ",")
		runningCount++
		fmt.Printf("Transactions finished: %v\n", runningCount)
		startTransaction := time.Now()
		switch info[0] {
		case "N":
			newOrderParser(info, buff)
		case "P":
			paymentParser(info)
		case "D":
			deliveryParser(info)
		case "O":
			orderStatusParser(info)
		case "S":
			stockLevelParser(info)
		case "I":
			popularItemParser(info)
		case "T":
			topBalanceParser()
		case "R":
			//relatedCustomerParser(info)
		}
		latencies = append(latencies, time.Since(startTransaction))
	}
	fmt.Println("Transactions finished")
	totalExecutionTime := time.Since(start)
	executionSeconds := totalExecutionTime.Seconds()
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	var sumLatency int64
	for _, t := range latencies {
		sumLatency += t.Milliseconds()
	}
	if executedTransactions > 0 {
		latencyAverage := sumLatency / int64(executedTransactions)
		latencyMedian := latencies[executedTransactions/2].Milliseconds()
		latency95Percent := latencies[int(float32(executedTransactions)*0.95)].Milliseconds()
		latency99Percent := latencies[int(float32(executedTransactions)*0.99)].Milliseconds()

		err := exportTransactionDetails(
			filepath, executedTransactions, executionSeconds,
			latencyAverage, latencyMedian, latency95Percent, latency99Percent,
		)
		if err != nil {
			fmt.Printf("client %v, total number of transactions processed: %v\n", clientNumber, executedTransactions)
			fmt.Printf("client %v, total excution time: %v s\n", clientNumber, executionSeconds)
			fmt.Printf("client %v, transaction throughput: %v per second\n", clientNumber, float32(executedTransactions)/float32(executionSeconds))
			fmt.Printf("client %v, Average transaction latency: %v ms\n", clientNumber, latencyAverage)
			fmt.Printf("client %v, median transaction latency: %v ms\n", clientNumber, latencyMedian)
			fmt.Printf("client %v, 95th percentile transaction latency: %v ms\n", clientNumber, latency95Percent)
			fmt.Printf("client %v, 99th percentile transaction latency: %v ms\n", clientNumber, latency99Percent)
		}
	}
}

func exportTransactionDetails(
	transactionFilePath string, executedTransactions int, executionSeconds float64,
	latencyAverage, latencyMedian, latency95Percent, latency99Percent int64,
) error {
	path := strings.Split(transactionFilePath, "/")
	transactionFileNumber := strings.Split(path[len(path)-1], ".")[0]
	outputFilePath := fmt.Sprintf("output/postgres/%v.json", transactionFileNumber)
	outFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Printf("Create json file error: %v\n", err)
		return nil
	}
	defer outFile.Close()

	data := struct {
		ExecutedNumber   int
		ExecutionTime    float64
		Throughput       float32
		LatencyAverage   int64
		LatencyMedian    int64
		Latency95Percent int64
		Latency99Percent int64
	}{
		ExecutedNumber:   executedTransactions,
		ExecutionTime:    executionSeconds,
		Throughput:       float32(executedTransactions) / float32(executionSeconds),
		LatencyAverage:   latencyAverage,
		LatencyMedian:    latencyMedian,
		Latency95Percent: latency95Percent,
		Latency99Percent: latency99Percent,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Marshal data to json error: %v\n", err)
		return err
	}
	err = ioutil.WriteFile(outputFilePath, jsonData, 0644)
	if err != nil {
		log.Printf("Write to json file error: %v\n", err)
		return err
	}

	return nil
}

func newOrderParser(info []string, buff *bufio.Reader) {
	customerId, _ := strconv.ParseUint(info[1], 10, 64)
	warehouseId, _ := strconv.ParseUint(info[2], 10, 64)
	districtId, _ := strconv.ParseUint(info[3], 10, 64)
	total, _ := strconv.Atoi(strings.Replace(info[4], "\r", "", -1))
	itemNumbers := make([]uint64, total)
	supplierWarehouses := make([]uint64, total)
	quantities := make([]int, total)

	for i := 0; i < total; i++ {
		subLine, _ := buff.ReadString('\n')
		subInfo := strings.Split(strings.Replace(subLine, "\n", "", -1), ",")
		itemNumber, _ := strconv.ParseUint(subInfo[0], 10, 64)
		supplyWarehouseId, _ := strconv.ParseUint(subInfo[1], 10, 64)
		quantity, _ := strconv.Atoi(strings.Replace(subInfo[2], "\r", "", -1))
		itemNumbers[i] = itemNumber
		supplierWarehouses[i] = supplyWarehouseId
		quantities[i] = quantity
	}

	err := postgre.NewOrderTransaction(warehouseId, districtId, customerId, uint64(total), itemNumbers, supplierWarehouses, quantities)
	if err != nil {
		fmt.Printf("New Order Transaction failed: %s\n", err.Error())
	}

}

func paymentParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	customerId, _ := strconv.ParseUint(info[3], 10, 64)
	payment, _ := strconv.ParseFloat(strings.Replace(info[4], "\r", "", -1), 32)
	err := postgre.PaymentTransaction(warehouseId, districtId, customerId, payment)
	if err != nil {
		fmt.Printf("Payment Transaction failed: %s\n", err.Error())
	}
}

func deliveryParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	carrierId, _ := strconv.ParseUint(strings.Replace(info[2], "\r", "", -1), 10, 64)
	fmt.Printf("the carrier : %d", carrierId)
	err := postgre.DeliveryTransaction(warehouseId, carrierId)
	if err != nil {
		fmt.Printf("Delivery Transaction failed: %s\n", err.Error())
	}
}

func orderStatusParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	customerId, _ := strconv.ParseUint(strings.Replace(info[3], "\r", "", -1), 10, 64)
	err := postgre.OrderStatusTransaction(warehouseId, districtId, customerId)
	if err != nil {
		fmt.Printf("Order-Status Transaction failed: %s\n", err.Error())
	}
}

func stockLevelParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	threshold, _ := strconv.Atoi(info[3])
	orderNumber, _ := strconv.Atoi(strings.Replace(info[4], "\r", "", -1))
	err := postgre.StockLevel(warehouseId, districtId, threshold, orderNumber)
	if err != nil {
		fmt.Printf("Stock-Level Transaction failed: %s\n", err.Error())
	}
}

func popularItemParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	orderNumber, _ := strconv.Atoi(strings.Replace(info[3], "\r", "", -1))
	err := postgre.PopularItem(warehouseId, districtId, orderNumber)
	if err != nil {
		fmt.Printf("Popular-Item Transaction failed: %s\n", err.Error())
	}
}

func topBalanceParser() {
	err := postgre.Top10Balance()
	if err != nil {
		fmt.Printf("Top-Balance Transaction failed: %s\n", err.Error())
	}
}

func relatedCustomerParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	customerId, _ := strconv.ParseUint(strings.Replace(info[3], "\r", "", -1), 10, 64)
	err := postgre.RelatedCustomerTransaction(customerId, warehouseId, districtId)
	if err != nil {
		fmt.Printf("Related-Customer Transaction failed: %s\n", err.Error())
	}
}
