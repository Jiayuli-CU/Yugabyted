package data

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"time"

	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

var db = postgre.GetDB()

func LoadWarehouse() {
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

	err = db.Create(&warehouses).Error
}

func LoadDistrict() {
	file, err := os.Open("./data_files/district.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// 设置返回记录中每行数据期望的字段数，-1 表示返回所有字段
	reader.FieldsPerRecord = -1
	// 通过 readAll 方法返回 csv 文件中的所有内容
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var districts []models.District
	districts = make([]models.District, len(records))
	for i, record := range records {
		wareHouseId, _ := strconv.ParseUint(record[0], 10, 64)
		id, _ := strconv.ParseUint(record[1], 10, 64)
		taxRate, _ := strconv.ParseFloat(record[8], 32)
		yearToDateAmount, _ := strconv.ParseFloat(record[9], 32)
		nextAvailableOrderNumber, _ := strconv.ParseUint(record[10], 10, 64)

		districts[i] = models.District{
			Id:                       id,
			WarehouseId:              wareHouseId,
			Name:                     record[2],
			StreetLine1:              record[3],
			StreetLine2:              record[4],
			City:                     record[5],
			State:                    record[6],
			Zip:                      record[7],
			TaxRate:                  taxRate,
			YearToDateAmount:         yearToDateAmount,
			NextAvailableOrderNumber: nextAvailableOrderNumber,
		}
	}

	err = db.CreateInBatches(&districts, 100).Error
}

func LoadCustomer() {
	file, err := os.Open("./data_files/customer.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var customers []models.Customer
	customers = make([]models.Customer, len(records))

	for i, record := range records {
		wareHouseId, _ := strconv.ParseUint(record[0], 10, 64)
		districtId, _ := strconv.ParseUint(record[1], 10, 64)
		id, _ := strconv.ParseUint(record[2], 10, 64)
		createTime, _ := time.ParseInLocation("2006-01-02 15:04:05", record[12], time.Local)
		creditLimit, _ := strconv.ParseFloat(record[14], 32)
		discountRate, _ := strconv.ParseFloat(record[15], 32)
		balance, _ := strconv.ParseFloat(record[16], 32)
		yearToDatePayment, _ := strconv.ParseFloat(record[17], 32)
		paymentsNumber, _ := strconv.ParseUint(record[18], 10, 64)
		deliveriesNumber, _ := strconv.ParseUint(record[19], 10, 64)

		customers[i] = models.Customer{
			Id:                id,
			WarehouseId:       wareHouseId,
			DistrictId:        districtId,
			FirstName:         record[3],
			MiddleName:        record[4],
			LastName:          record[5],
			StreetLine1:       record[6],
			StreetLine2:       record[7],
			City:              record[8],
			State:             record[9],
			Zip:               record[10],
			Phone:             record[11],
			CreateTime:        createTime,
			CreditStatus:      record[13],
			CreditLimit:       creditLimit,
			DiscountRate:      discountRate,
			Balance:           balance,
			YearToDatePayment: yearToDatePayment,
			PaymentsNumber:    paymentsNumber,
			DeliveriesNumber:  deliveriesNumber,
			MiscellaneousData: record[20],
		}
	}

	err = db.CreateInBatches(&customers, 100).Error
}

func LoadItem() {
	file, err := os.Open("./data_files/item.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var items []models.Item
	items = make([]models.Item, len(records))
	fmt.Println(len(records))

	for i, record := range records {
		id, _ := strconv.ParseUint(record[0], 10, 64)
		price, _ := strconv.ParseFloat(record[2], 32)
		imageId, _ := strconv.ParseUint(record[3], 10, 64)

		items[i] = models.Item{
			Id:      id,
			Name:    record[1],
			Price:   price,
			ImageId: imageId,
			Data:    record[4],
		}
	}

	err = db.CreateInBatches(&items, 100).Error
}

func LoadStock() {
	file, err := os.Open("./data_files/stock.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// 设置返回记录中每行数据期望的字段数，-1 表示返回所有字段
	reader.FieldsPerRecord = -1
	// 通过 readAll 方法返回 csv 文件中的所有内容
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var stocks []models.Stock
	stocks = make([]models.Stock, len(records))
	for i, record := range records {
		wareHouseId, _ := strconv.ParseUint(record[0], 10, 64)
		itemId, _ := strconv.ParseUint(record[1], 10, 64)
		quantity, _ := strconv.ParseInt(record[2], 10, 64)
		yearToDateQuantityOrdered, _ := strconv.ParseInt(record[3], 10, 64)
		ordersNumber, _ := strconv.ParseUint(record[4], 10, 64)
		remoteOrdersNumber, _ := strconv.ParseUint(record[5], 10, 64)

		stocks[i] = models.Stock{
			WarehouseId:               wareHouseId,
			ItemId:                    itemId,
			Quantity:                  int(quantity),
			YearToDateQuantityOrdered: int(yearToDateQuantityOrdered),
			OrdersNumber:              ordersNumber,
			RemoteOrdersNumber:        remoteOrdersNumber,
			District1Info:             record[6],
			District2Info:             record[7],
			District3Info:             record[8],
			District4Info:             record[9],
			District5Info:             record[10],
			District6Info:             record[11],
			District7Info:             record[12],
			District8Info:             record[13],
			District9Info:             record[14],
			District10Info:            record[15],
			MiscellaneousData:         record[16],
		}
	}

	err = db.CreateInBatches(&stocks, 100).Error
}

func LoadOrder() {

	var err error

	file, err := os.Open("./data_files/order.csv")
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

	orders := make([]models.Order, len(record))

	var warehouseId, districtId, id, customerId, carrierId, itemNumber uint64

	for i, o := range record {
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
		entryTime, _ := time.ParseInLocation("2006-01-02 15:04:05", o[7], time.Local)
		status, _ := strconv.ParseUint(o[6], 10, 64)

		orders[i] = models.Order{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			Id:          id,
			CustomerId:  customerId,
			CarrierId:   carrierId,
			ItemsNumber: itemNumber,
			Status:      int(status),
			EntryTime:   entryTime,
		}
	}

	err = db.CreateInBatches(&orders, 100).Error
}

func LoadOrderLine() error {
	file, err := os.Open("./data_files/order-line.csv")
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

	orderlines := make([]models.OrderLine, len(record))

	// var warehouseId, districtId, orderId, id, itemId, supplyWarehouseId uint64
	// var quantity int
	// var deliveryTime
	// var totalPrice float64
	var deliveryTime time.Time

	for i, ol := range record {
		warehouseId, _ := strconv.ParseUint(ol[0], 10, 64)
		districtId, _ := strconv.ParseUint(ol[1], 10, 64)
		orderId, _ := strconv.ParseUint(ol[2], 10, 64)
		id, _ := strconv.ParseUint(ol[3], 10, 64)
		itemId, _ := strconv.ParseUint(ol[4], 10, 64)
		if ol[5] != "" {
			deliveryTime, _ = time.ParseInLocation("2006-01-02 15:04:05", ol[5], time.Local)
		} else {
			deliveryTime = time.Time{}
		}
		totalPrice, _ := strconv.ParseFloat(ol[6], 32)
		supplyWarehouseId, _ := strconv.ParseUint(ol[7], 10, 64)
		quantity, _ := strconv.ParseInt(ol[8], 10, 64)

		orderlines[i] = models.OrderLine{
			WarehouseId:       warehouseId,
			DistrictId:        districtId,
			OrderId:           orderId,
			Id:                id,
			ItemId:            itemId,
			DeliveryTime:      deliveryTime,
			Price:             totalPrice,
			SupplyNumber:      supplyWarehouseId,
			Quantity:          int(quantity),
			MiscellaneousData: ol[9],
		}
	}

	err = db.CreateInBatches(&orderlines, 100).Error

	return err
}
