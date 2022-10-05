package data

import (
	"cs5424project/store/models"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

func loadWarehouse() {
	file, err := os.Open("./data_files/warehouse.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// 设置返回记录中每行数据期望的字段数，-1 表示返回所有字段
	reader.FieldsPerRecord = -1
	// 通过 readAll 方法返回 csv 文件中的所有内容
	record, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var warehouses []models.Warehouse
	warehouses = make([]models.Warehouse, len(record))
	for i, w := range record {
		id, _ := strconv.ParseUint(w[0], 10, 64)
		taxRate, _ := strconv.ParseFloat(w[7], 32)
		yearToDateAmount, _ := strconv.ParseFloat(w[8], 32)
		warehouses[i] = models.Warehouse{
			Id:               id,
			Name:             w[1],
			StreetLine1:      w[2],
			StreetLine2:      w[3],
			City:             w[4],
			State:            w[5],
			Zip:              w[6],
			TaxRate:          taxRate,
			YearToDateAmount: yearToDateAmount,
		}
	}
	fmt.Println(warehouses[1])
}
