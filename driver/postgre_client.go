package driver

import (
	"bufio"
	"cs5424project/transaction"
	"cs5424project/transaction/postgre"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func SqlClient(filepath string) {
	file, err := os.Open(filepath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	buff := bufio.NewReader(file)
	for {
		line, err := buff.ReadString('\n')
		if err == io.EOF {
			break
		}
		info := strings.Split(line, " ")
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
	}
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
	err := transaction.DeliveryTransaction(warehouseId, carrierId)
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
