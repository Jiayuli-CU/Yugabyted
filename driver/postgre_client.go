package driver

import (
	"bufio"
	"cs5424project/transaction/postgre"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func SqlClient(filepath string, clientNumber int) {
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewReader(file)
	excutedTransactions := 0
	var latencies []time.Duration
	start := time.Now()
	for {
		line, err := buff.ReadString('\n')
		if err == io.EOF {
			break
		}
		excutedTransactions += 1
		info := strings.Split(line, ",")
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
		}
		latencies = append(latencies, time.Since(startTransaction))
	}
	totalExcutionTime := time.Since(start)
	fmt.Printf("client %v, total excution time is %v\n", clientNumber, totalExcutionTime)
}

func newOrderParser(info []string, buff *bufio.Reader) {
	customerId, _ := strconv.ParseUint(info[1], 10, 64)
	warehouseId, _ := strconv.ParseUint(info[2], 10, 64)
	districtId, _ := strconv.ParseUint(info[3], 10, 64)
	total, _ := strconv.ParseUint(info[4], 10, 64)
	itemNumbers := make([]uint64, total)
	supplierWarehouses := make([]uint64, total)
	quantities := make([]int, total)
	for i := 0; i < int(total); i++ {
		subLine, _ := buff.ReadString('\n')
		subInfo := strings.Split(subLine, " ")
		itemNumber, _ := strconv.ParseUint(subInfo[0], 10, 64)
		supplyWarehouseId, _ := strconv.ParseUint(subInfo[1], 10, 64)
		quantity, _ := strconv.Atoi(subInfo[2])
		itemNumbers = append(itemNumbers, itemNumber)
		supplierWarehouses = append(supplierWarehouses, supplyWarehouseId)
		quantities = append(quantities, quantity)
	}
	err := postgre.NewOrder(warehouseId, districtId, customerId, total, itemNumbers, supplierWarehouses, quantities)
	if err != nil {
		fmt.Printf("New Order Transaction failed: %s\n", err.Error())
	}

}

func paymentParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	customerId, _ := strconv.ParseUint(info[3], 10, 64)
	payment, _ := strconv.ParseFloat(info[4], 32)
	err := postgre.PaymentTransaction(warehouseId, districtId, customerId, payment)
	if err != nil {
		fmt.Printf("Payment Transaction failed: %s\n", err.Error())
	}
}

func deliveryParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	carrierId, _ := strconv.ParseUint(info[2], 10, 64)
	err := postgre.DeliveryTransaction(warehouseId, carrierId)
	if err != nil {
		fmt.Printf("Delivery Transaction failed: %s\n", err.Error())
	}
}

func orderStatusParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	customerId, _ := strconv.ParseUint(info[3], 10, 64)
	err := postgre.OrderStatusTransaction(warehouseId, districtId, customerId)
	if err != nil {
		fmt.Printf("Order-Status Transaction failed: %s\n", err.Error())
	}
}

func stockLevelParser(info []string) {
	warehouseId, _ := strconv.ParseUint(info[1], 10, 64)
	districtId, _ := strconv.ParseUint(info[2], 10, 64)
	threshold, _ := strconv.Atoi(info[3])
	orderNumber, _ := strconv.Atoi(info[4])
	err := postgre.StockLevel(warehouseId, districtId, threshold, orderNumber)
	if err != nil {
		fmt.Printf("Stock-Level Transaction failed: %s\n", err.Error())
	}
}
