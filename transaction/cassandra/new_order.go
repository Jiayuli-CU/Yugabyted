package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

func NewOrder(ctx context.Context, warehouseId, districtId, customerId, total int, itemNumbers, supplierWarehouses []int, quantities []int) error {

	var warehouseTax, districtTax, discount float32
	var totalAmountInt int
	var orderLines []cassandra.OrderLine
	var orderId int
	var err error
	var customerBasicInfo cassandra.CustomerInfo

	local := 1
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = 0
			break
		}
	}

	err = session.Query(`SELECT warehouse_tax_rate, district_tax_rate, next_order_number FROM cs5424_groupI.districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).WithContext(ctx).
		Scan(&warehouseTax, &districtTax, &orderId)
	if err != nil {
		log.Printf("Find district error: %v\n", err)
		return err
	}

	//CAS to handle concurrent read and write
	for {
		applied, err := session.Query(`UPDATE cs5424_groupI.districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, orderId+1, warehouseId, districtId, orderId).
			WithContext(ctx).ScanCAS(nil, nil, &orderId)
		if applied && err == nil {
			break
		}
	}

	if err = session.Query(`SELECT discount_rate, basic_info FROM cs5424_groupI.customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		WithContext(ctx).Scan(&discount, &customerBasicInfo); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}

	customerInfo := CustomerInfoForNewOrder{
		CustomerIdentifier: CustomerIdentifier{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			CustomerId:  customerId,
		},
		LastName: customerBasicInfo.LastName,
		Credit:   customerBasicInfo.Credit,
		Discount: discount,
	}

	var itemPrice float32
	var itemName string

	itemInfos := make([]ItemInfo, total)

	for idx, itemNumber := range itemNumbers {

		wId := supplierWarehouses[idx]
		quantity := quantities[idx]

		//update stock info
		b := session.NewBatch(gocql.CounterBatch).WithContext(ctx)
		var stmt string
		if wId == warehouseId {
			stmt = "UPDATE cs5424_groupI.stock_counters SET quantity = quantity - ?, order_count = order_count + 1 WHERE warehouse_id = ? AND item_id = ?"
		} else {
			stmt = "UPDATE cs5424_groupI.stock_counters SET quantity = quantity - ?, order_count = order_count + 1, remote_count = remote_count + 1 WHERE warehouse_id = ? AND item_id = ?"
		}
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       stmt,
			Args:       []interface{}{quantity, warehouseId, itemNumber},
			Idempotent: false,
		})
		err = session.ExecuteBatch(b)
		if err != nil {
			return err
		}
		var stockQuantity int
		if err = session.Query(`SELECT quantity FROM cs5424_groupI.stock_counters WHERE warehouse_id = ? AND item_id = ?`, warehouseId, itemNumber).WithContext(ctx).Scan(&stockQuantity); err != nil {
			log.Printf("Find quantity error: %v\n", err)
			return err
		}
		if stockQuantity < 10 {
			if err = session.Query(`UPDATE cs5424_groupI.stock_counters SET quantity = quantity + 100 WHERE warehouse_id = ? AND item_id = ?`, warehouseId, itemNumber).WithContext(ctx).Exec(); err != nil {
				log.Printf("Find quantity error: %v\n", err)
				return err
			}
			stockQuantity += 100
		}

		// calculate item and total amount
		if err = session.Query(`SELECT item_name, item_price FROM cs5424_groupI.items WHERE item_id = ? LIMIT 1`, itemNumber).WithContext(ctx).Scan(&itemName, &itemPrice); err != nil {
			log.Printf("Find item error: %v\n", err)
			return err
		}
		itemAmountInt := quantity * int(itemPrice*100)
		totalAmountInt += itemAmountInt

		orderLine := cassandra.OrderLine{
			ItemName:          itemName,
			OrderLineId:       idx + 1,
			ItemId:            itemNumber,
			SupplyWarehouseId: wId,
			Quantity:          quantity,
			AmountInt:         itemAmountInt,
			MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
		}
		orderLines = append(orderLines, orderLine)

		itemInfo := ItemInfo{
			ItemName:            itemName,
			ItemNumber:          itemNumber,
			SupplierWarehouseId: wId,
			Quantity:            quantity,
			OrderLineAmount:     itemPrice,
			StockQuantity:       stockQuantity,
		}

		itemInfos = append(itemInfos, itemInfo)
	}

	entryTime := time.Now()

	if err = session.Query(`INSERT INTO cs5424_groupI.orders (warehouse_id, district_id, order_id, customer_id, first_name, middle_name, last_name, items_number, all_local, entry_time, order_lines, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		warehouseId, districtId, orderId, customerId, customerBasicInfo.FirstName, customerBasicInfo.MiddleName, customerBasicInfo.LastName, total, local, entryTime, orderLines, totalAmountInt).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	if err = session.Query(`UPDATE cs5424_groupI.customers SET last_order_id = ? WHERE warehouse_id =? AND district_id = ? AND customer_id = ?`, orderId, warehouseId, districtId, customerId).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	totalAmount := float32(totalAmountInt) / 100 * (1 + warehouseTax + districtTax) * (1 - discount)

	output := NewOrderTransactionOutput{
		TransactionType:  "New Order Transaction",
		CustomerInfo:     customerInfo,
		WarehouseTaxRate: warehouseTax,
		DistrictTaxRate:  districtTax,
		OrderNumber:      orderId,
		ItemNumbers:      total,
		TotalAmount:      totalAmount,
		ItemInfo:         itemInfos,
		EntryDate:        entryTime,
	}

	fmt.Printf("%+v\n", output)
	println()

	return nil
}
