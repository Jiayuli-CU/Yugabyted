package data

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

func LoadWarehouse() error {
	file, err := os.Open("data/data_files/warehouse.csv")
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

	//var warehouses []models.Warehouse
	warehouses := make([]models.Warehouse, len(record))
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

	db := postgre.GetDB()
	if err = db.Create(&warehouses).Error; err != nil {
		return err
	}
	return nil
}

func LoadOrder() error {

	var err error

	file, err := os.Open("data/data_files/order.csv")
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

	//db := postgre.GetDB()

	orders := make([]models.Order, 100)

	var warehouseId, districtId, id, customerId, carrierId, itemNumber uint64

	index := 0
	for _, o := range record {
		if index == 100 {
			//if err = db.Create(&orders).Error; err != nil {
			//	return err
			//}
			fmt.Println(orders[0])
			index = 0
			break
		}
		warehouseId, _ = strconv.ParseUint(o[0], 10, 64)
		districtId, _ = strconv.ParseUint(o[1], 10, 64)
		id, _ = strconv.ParseUint(o[2], 10, 64)
		customerId, _ = strconv.ParseUint(o[3], 10, 64)
		if o[4] != "null" {
			carrierId, _ = strconv.ParseUint(o[4], 10, 64)
		} else {
			carrierId = 0
		}
		itemNumber, _ = strconv.ParseUint(o[5], 10, 64)

		orders[index] = models.Order{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			Id:          id,
			CustomerId:  customerId,
			CarrierId:   carrierId,
			ItemsNumber: itemNumber,
			Status:      o[6] != "0",
			EntryTime:   time.Now(),
		}

		index += 1
	}

	return nil
}
