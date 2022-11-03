package data

import (
	"cs5424project/store/cassandra"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"os"
	"strconv"
	"time"
)

var session = cassandra.GetSession()

func CqlDataLoader() {
	warehouses := parseAndLoadWarehouse()
	loadWarehouseCounter(warehouses)
	districts, districtsCounter := parseDistrictAndCounter(warehouses)
	items := parseItem()
	loadItem(items)
	customers, customerCounters := parseCustomerAndCounter()
	orders := parseOrderAndUpdateCustomer(customers)
	parseOrderLineAndUpdateDistrict(orders, items, districts)

	loadDistrictAndCounter(districts, districtsCounter)
	loadCustomerAndCounter(customers, customerCounters)
	loadOrder(orders)
	parseAndLoadStock()
}

func parseAndLoadWarehouse() []cassandra.Warehouse {
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

	warehouses := make([]cassandra.Warehouse, len(record))
	for _, w := range record {
		id, _ := strconv.Atoi(w[0])
		taxRate, _ := strconv.ParseFloat(w[7], 32)
		yearToDateAmount, _ := strconv.ParseFloat(w[8], 32)
		warehouseBasicInfo := cassandra.WarehouseBasicInfo{
			Name:    w[1],
			Street1: w[2],
			Street2: w[3],
			City:    w[4],
			State:   w[5],
			Zip:     w[6],
		}
		warehouses[id-1] = cassandra.Warehouse{
			Id:                 id,
			WarehouseBasicInfo: warehouseBasicInfo,
			TaxRate:            float32(taxRate),
			YearToDateAmount:   float32(yearToDateAmount),
		}
	}

	return warehouses
}

func loadWarehouseCounter(warehouses []cassandra.Warehouse) {
	var err error
	for _, warehouse := range warehouses {
		fmt.Println(warehouse.Id)
		err = session.Query(`UPDATE cs5424_groupI.warehouse_counter SET warehouse_year_to_date_payment = warehouse_year_to_date_payment + ? WHERE warehouse_id = ?`, int(warehouse.YearToDateAmount*100), warehouse.Id).Exec()
		if err != nil {
			fmt.Println(err)
		}
	}
}

func parseDistrictAndCounter(warehouses []cassandra.Warehouse) ([][]cassandra.District, [][]int) {
	file, err := os.Open("data/data_files/district.csv")
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

	var districts [][]cassandra.District
	var districtsCounter [][]int
	districts = make([][]cassandra.District, len(warehouses))
	districtsCounter = make([][]int, len(warehouses))

	for _, record := range records {
		warehouseId, _ := strconv.Atoi(record[0])
		id, _ := strconv.Atoi(record[1])
		taxRate, _ := strconv.ParseFloat(record[8], 32)
		yearToDateAmount, _ := strconv.ParseFloat(record[9], 32)
		nextAvailableOrderNumber, _ := strconv.Atoi(record[10])

		districtInfo := cassandra.DistrictInfo{
			Name:    record[2],
			Street1: record[3],
			Street2: record[4],
			City:    record[5],
			State:   record[6],
			Zip:     record[7],
		}

		districts[warehouseId-1] = append(districts[warehouseId-1], cassandra.District{
			DistrictId:       id,
			WarehouseId:      warehouseId,
			DistrictInfo:     districtInfo,
			WarehouseInfo:    warehouses[warehouseId-1].WarehouseBasicInfo,
			DistrictTaxRate:  float32(taxRate),
			WarehouseTaxRate: warehouses[warehouseId-1].TaxRate,
			NextOrderNumber:  nextAvailableOrderNumber,
		})

		districtsCounter[warehouseId-1] = append(districtsCounter[warehouseId-1], int(yearToDateAmount*100))
	}

	return districts, districtsCounter
}

func loadDistrictAndCounter(districts [][]cassandra.District, districtsCounter [][]int) {
	for w, subDistricts := range districts {
		b := session.NewBatch(gocql.UnloggedBatch)
		for d, district := range subDistricts {
			districtJson, _ := json.Marshal(district)
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt:       "INSERT INTO cs5424_groupi.districts JSON ?",
				Args:       []interface{}{string(districtJson)},
				Idempotent: true,
			})
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt:       "UPDATE cs5424_groupi.district_counter SET district_year_to_date_payment = district_year_to_date_payment + ? WHERE warehouse_id = ? AND district_id = ?",
				Args:       []interface{}{districtsCounter[w][d], district.WarehouseId, district.DistrictId},
				Idempotent: false,
			})
		}
		err := session.ExecuteBatch(b)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func parseCustomerAndCounter() ([][][]cassandra.Customer, [][][]cassandra.CustomerCounter) {
	file, err := os.Open("data/data_files/customer.csv")
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

	var customers [][][]cassandra.Customer
	var customerCounters [][][]cassandra.CustomerCounter
	customers = make([][][]cassandra.Customer, 10)
	customerCounters = make([][][]cassandra.CustomerCounter, 10)
	for i := 0; i < 10; i++ {
		customers[i] = make([][]cassandra.Customer, 10)
		customerCounters[i] = make([][]cassandra.CustomerCounter, 10)
	}

	for _, record := range records {
		wareHouseId, _ := strconv.Atoi(record[0])
		districtId, _ := strconv.Atoi(record[1])
		customerId, _ := strconv.Atoi(record[2])
		createTime, _ := time.ParseInLocation("2006-01-02 15:04:05", record[12], time.Local)
		creditLimit, _ := strconv.ParseFloat(record[14], 32)
		discountRate, _ := strconv.ParseFloat(record[15], 32)
		balance, _ := strconv.ParseFloat(record[16], 32)
		yearToDatePayment, _ := strconv.ParseFloat(record[17], 32)
		paymentsNumber, _ := strconv.Atoi(record[18])
		deliveriesNumber, _ := strconv.Atoi(record[19])

		customerInfo := cassandra.CustomerInfo{
			FirstName:   record[3],
			MiddleName:  record[4],
			LastName:    record[5],
			Street1:     record[6],
			Street2:     record[7],
			City:        record[8],
			State:       record[9],
			Zip:         record[10],
			Phone:       record[11],
			Since:       createTime,
			Credit:      record[13],
			CreditLimit: float32(creditLimit),
		}
		customers[wareHouseId-1][districtId-1] = append(customers[wareHouseId-1][districtId-1],
			cassandra.Customer{
				WarehouseId:       wareHouseId,
				DistrictId:        districtId,
				CustomerId:        customerId,
				BasicInfo:         customerInfo,
				DiscountRate:      float32(discountRate),
				MiscellaneousData: record[20],
			})
		customerCounters[wareHouseId-1][districtId-1] = append(customerCounters[wareHouseId-1][districtId-1],
			cassandra.CustomerCounter{
				Balance:           int(balance * 100),
				YearToDatePayment: int(yearToDatePayment * 100),
				PaymentCount:      paymentsNumber,
				DeliveryCount:     deliveriesNumber,
			})
	}
	return customers, customerCounters
}

func loadCustomerAndCounter(customers [][][]cassandra.Customer, customerCounters [][][]cassandra.CustomerCounter) {
	var err error

	for w, customer2Layer := range customers {
		for d, customer3Layer := range customer2Layer {
			var b = session.NewBatch(gocql.UnloggedBatch)
			for c, customer := range customer3Layer {
				if c != 0 && c%1000 == 0 {
					err = session.ExecuteBatch(b)
					if err != nil {
						fmt.Println(err)
						return
					}
					b = session.NewBatch(gocql.UnloggedBatch)
					fmt.Printf("current state: %v, %v, %v\n", w, d, c)
				}
				customerJson, _ := json.Marshal(customer)
				customerCounter := customerCounters[w][d][c]
				b.Entries = append(b.Entries, gocql.BatchEntry{
					Stmt:       "INSERT INTO cs5424_groupi.customers JSON ?",
					Args:       []interface{}{string(customerJson)},
					Idempotent: true,
				})
				b.Entries = append(b.Entries, gocql.BatchEntry{
					Stmt: "UPDATE cs5424_groupi.customer_counters SET " +
						"payment_count = payment_count + ?, delivery_count = delivery_count + ?, " +
						"balance = balance + ?, year_to_date_payment = year_to_date_payment + ? " +
						"WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?",
					Args: []interface{}{
						customerCounter.PaymentCount,
						customerCounter.DeliveryCount,
						customerCounter.Balance,
						customerCounter.YearToDatePayment,
						customer.WarehouseId,
						customer.DistrictId,
						customer.CustomerId,
					},
					Idempotent: false,
				})
			}
			err = session.ExecuteBatch(b)
			fmt.Printf("current state: %v, %v\n", w, d)
			if err != nil {
				fmt.Println(err)
				return
			}

		}
	}
}

func parseItem() []cassandra.Item {
	file, err := os.Open("data/data_files/item.csv")
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

	var items []cassandra.Item
	items = make([]cassandra.Item, len(records))

	for i, record := range records {
		id, _ := strconv.Atoi(record[0])
		price, _ := strconv.ParseFloat(record[2], 32)
		imageId, _ := strconv.Atoi(record[3])

		items[i] = cassandra.Item{
			ItemId:              id,
			ItemName:            record[1],
			ItemPrice:           float32(price),
			ItemImageIdentifier: imageId,
			ItemData:            record[4],
		}
	}
	return items
}

func loadItem(items []cassandra.Item) {

	var b = session.NewBatch(gocql.UnloggedBatch)
	var err error

	for i, item := range items {
		if i != 0 && i%1000 == 0 {
			err = session.ExecuteBatch(b)
			if err != nil {
				fmt.Printf("mid batch failed: item_id:%v, err: %v\n", i, err)
				return
			}
			b = session.NewBatch(gocql.UnloggedBatch)
			fmt.Printf("current state: %v\n", i)
		}
		itemJson, err := json.Marshal(item)
		if err != nil {
			fmt.Printf("Json parser error: %v\n", err)
		}
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "INSERT INTO cs5424_groupi.items JSON ?",
			Args:       []interface{}{string(itemJson)},
			Idempotent: true,
		})
	}
	err = session.ExecuteBatch(b)
	if err != nil {
		fmt.Printf("final batch failed: err: %v\n", err)
		return
	}
}

func parseOrderLineAndUpdateDistrict(orders [][][]cassandra.Order, items []cassandra.Item, districts [][]cassandra.District) {
	file, err := os.Open("data/data_files/order-line.csv")
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

	var deliveryTime time.Time

	for _, ol := range record {

		warehouseId, _ := strconv.Atoi(ol[0])
		districtId, _ := strconv.Atoi(ol[1])
		orderId, _ := strconv.Atoi(ol[2])
		id, _ := strconv.Atoi(ol[3])
		itemId, _ := strconv.Atoi(ol[4])
		if ol[5] != "null" {
			deliveryTime, _ = time.ParseInLocation("2006-01-02 15:04:05", ol[5], time.Local)
		}
		totalPrice, _ := strconv.ParseFloat(ol[6], 32)
		supplyWarehouseId, _ := strconv.Atoi(ol[7])
		quantity, _ := strconv.Atoi(ol[8])

		orderLine := cassandra.OrderLine{
			OrderLineId:       id,
			ItemId:            itemId,
			ItemName:          items[itemId-1].ItemName,
			AmountInt:         int(totalPrice * 100),
			SupplyWarehouseId: supplyWarehouseId,
			Quantity:          quantity,
			MiscellaneousData: ol[9],
		}
		if ol[5] != "null" {
			orders[warehouseId-1][districtId-1][orderId-1].DeliveryTime = &deliveryTime
			if orderId+1 > districts[warehouseId-1][districtId-1].NextDeliveryOrderId {
				districts[warehouseId-1][districtId-1].NextDeliveryOrderId = orderId + 1
			}
		}
		//orders[warehouseId-1][districtId-1][orderId-1].DeliveryTime = deliveryTime
		orders[warehouseId-1][districtId-1][orderId-1].TotalAmount += int(totalPrice * 100)
		orders[warehouseId-1][districtId-1][orderId-1].OrderLines = append(orders[warehouseId-1][districtId-1][orderId-1].OrderLines, orderLine)
	}

}

func parseOrderAndUpdateCustomer(customers [][][]cassandra.Customer) [][][]cassandra.Order {
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

	orders := make([][][]cassandra.Order, 10)
	for i, _ := range orders {
		orders[i] = make([][]cassandra.Order, 10)
	}

	for _, o := range record {
		warehouseId, _ := strconv.Atoi(o[0])
		districtId, _ := strconv.Atoi(o[1])
		orderId, _ := strconv.Atoi(o[2])
		customerId, _ := strconv.Atoi(o[3])
		carrierId := 0
		if o[4] != "null" {
			carrierId, _ = strconv.Atoi(o[4])
		}
		itemNumber, _ := strconv.Atoi(o[5])
		entryTime, _ := time.ParseInLocation("2006-01-02 15:04:05", o[7], time.Local)
		status, _ := strconv.Atoi(o[6])

		order := cassandra.Order{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			OrderId:     orderId,
			CustomerId:  customerId,
			FirstName:   customers[warehouseId-1][districtId-1][customerId-1].BasicInfo.FirstName,
			MiddleName:  customers[warehouseId-1][districtId-1][customerId-1].BasicInfo.MiddleName,
			LastName:    customers[warehouseId-1][districtId-1][customerId-1].BasicInfo.LastName,
			ItemsNumber: itemNumber,
			AllLocal:    status,
			EntryTime:   entryTime,
			OrderLines:  []cassandra.OrderLine{},
			TotalAmount: 0,

			CarrierId: carrierId,
		}

		if orderId > customers[warehouseId-1][districtId-1][customerId-1].LastOrderId {
			customers[warehouseId-1][districtId-1][customerId-1].LastOrderId = orderId
		}

		orders[warehouseId-1][districtId-1] = append(orders[warehouseId-1][districtId-1], order)
	}
	return orders
}

func loadOrder(orders [][][]cassandra.Order) {
	var err error
	for w, order2Layer := range orders {
		for d, order3Layer := range order2Layer {
			var b = session.NewBatch(gocql.UnloggedBatch)
			for o, order := range order3Layer {
				if o != 0 && o%1000 == 0 {
					err = session.ExecuteBatch(b)
					if err != nil {
						fmt.Printf("mid batch failed: warehouse id: %v, district_id: %v, order id:%v, err: %v\n", w, d, o, err)
						return
					}
					b = session.NewBatch(gocql.UnloggedBatch)
					fmt.Printf("current state: %v, %v, %v\n", w, d, o)
				}
				orderJson, err := json.Marshal(order)
				if err != nil {
					fmt.Printf("Json parser error: %v, w: %v, d: %v, o: %v\n", err, w, d, o)
				}
				b.Entries = append(b.Entries, gocql.BatchEntry{
					Stmt:       "INSERT INTO cs5424_groupi.orders JSON ?",
					Args:       []interface{}{string(orderJson)},
					Idempotent: true,
				})
			}
			err = session.ExecuteBatch(b)
			if err != nil {
				fmt.Printf("the last batch failed: %v\n", err)
				return
			}
			fmt.Printf("current state: %v, %v\n", w, d)

		}
	}
}

func parseAndLoadStock() {
	file, err := os.Open("data/data_files/stock.csv")
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

	var stocks [][]cassandra.Stock
	var stockCounters [][]cassandra.StockCounter
	stocks = make([][]cassandra.Stock, 10)
	stockCounters = make([][]cassandra.StockCounter, 10)

	for _, record := range records {
		wareHouseId, _ := strconv.Atoi(record[0])
		itemId, _ := strconv.Atoi(record[1])
		quantity, _ := strconv.Atoi(record[2])
		yearToDateQuantityOrdered, _ := strconv.Atoi(record[3])
		ordersNumber, _ := strconv.Atoi(record[4])
		remoteOrdersNumber, _ := strconv.Atoi(record[5])

		stockInfo := cassandra.StockInfo{
			District1Info:     record[6],
			District2Info:     record[7],
			District3Info:     record[8],
			District4Info:     record[9],
			District5Info:     record[10],
			District6Info:     record[11],
			District7Info:     record[12],
			District8Info:     record[13],
			District9Info:     record[14],
			District10Info:    record[15],
			MiscellaneousData: record[16],
		}

		stock := cassandra.Stock{
			WarehouseId: wareHouseId,
			ItemId:      itemId,
			BasicInfo:   stockInfo,
		}

		stockCounter := cassandra.StockCounter{
			WarehouseId:   wareHouseId,
			ItemId:        itemId,
			Quantity:      quantity,
			TotalQuantity: yearToDateQuantityOrdered,
			OrderCount:    ordersNumber,
			RemoteCount:   remoteOrdersNumber,
		}

		stocks[wareHouseId-1] = append(stocks[wareHouseId-1], stock)
		stockCounters[wareHouseId-1] = append(stockCounters[wareHouseId-1], stockCounter)

	}

	for w, stock1Layer := range stocks {
		var b = session.NewBatch(gocql.UnloggedBatch)
		for i, stock := range stock1Layer {
			if i != 0 && i%1000 == 0 {
				err = session.ExecuteBatch(b)
				if err != nil {
					fmt.Printf("mid batch failed: item id:%v, err: %v\n", i-1, err)
					return
				}
				b = session.NewBatch(gocql.UnloggedBatch)
				fmt.Printf("current state: %v, %v\n", w, i)
			}
			stockJson, _ := json.Marshal(&stock)
			if err != nil {
				fmt.Printf("stock json parse error: %v\n", err)
				return
			}
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt:       "INSERT INTO cs5424_groupi.stocks JSON ?",
				Args:       []interface{}{string(stockJson)},
				Idempotent: true,
			})
			stockCounter := stockCounters[w][i]
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: "UPDATE cs5424_groupi.stock_counters SET " +
					"order_count = order_count + ?, remote_count = remote_count + ?, quantity = quantity + ?, total_quantity = total_quantity + ? " +
					"WHERE warehouse_id = ? AND item_id = ?",
				Args:       []interface{}{stockCounter.OrderCount, stockCounter.RemoteCount, stockCounter.Quantity, stockCounter.TotalQuantity, stockCounter.WarehouseId, stockCounter.ItemId},
				Idempotent: false,
			})
		}

		err = session.ExecuteBatch(b)
		if err != nil {
			fmt.Printf("final batch failed: err: %v\n", err)
			return
		}
	}
}
