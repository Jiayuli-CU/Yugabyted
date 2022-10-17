package driver

import (
	"bufio"
	"cs5424project/transaction"
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
			err = transaction.NewOrder(warehouseId, districtId, customerId, total, itemNumbers, supplierWarehouses, quantities)
			if err != nil {
				fmt.Println(err)
			}
		}

	}
}
