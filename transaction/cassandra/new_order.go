package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

func NewOrder(warehouseId, districtId, customerId, total int, itemNumbers, supplierWarehouses []int, quantities []int) error {

	var warehouseTax, districtTax, discount, totalAmount float32
	var orderLines []cassandra.OrderLine
	var orderId int
	var err error

	local := 1
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = 0
			break
		}
	}

	ctx := context.Background()

	//CAS to handle concurrent read and write
	for {
		err = session.Query(`SELECT warehouse_tax, district_tax, next_order_number FROM districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).WithContext(ctx).Consistency(gocql.Quorum).
			Scan(&warehouseTax, &districtTax, &orderId)
		if err != nil {
			log.Printf("Find district error: %v\n", err)
			continue
		}

		err = session.Query(`UPDATE districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, orderId+1, warehouseId, districtId, orderId).
			WithContext(ctx).Exec()
		if err == nil {
			break
		}
	}

	if err = session.Query(`SELECT discount_rate FROM customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(&discount); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}

	var itemPrice float32

	for idx, itemNumber := range itemNumbers {

		wId := supplierWarehouses[idx]
		quantity := quantities[idx]

		//update stock info
		b := session.NewBatch(gocql.CounterBatch).WithContext(ctx)
		var stmt string
		if wId != warehouseId {
			stmt = "UPDATE stock_counter SET quantity = quantity - ?, order_count = order_count + 1 WHERE warehouse_id = ? AND item_id = ?"
		} else {
			stmt = "UPDATE stock_counter SET quantity = quantity - ?, order_count = order_count + 1, remote_count = remote_count + 1 WHERE warehouse_id = ? AND item_id = ?"
		}
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       stmt,
			Args:       []interface{}{quantity, warehouseId, itemNumber},
			Idempotent: false,
		})
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE stock_counter SET quantity = quantity + 100 WHERE warehouse_id = ? AND item_id = ? IF quantity < 10",
			Args:       []interface{}{warehouseId, itemNumber},
			Idempotent: false,
		})
		err = session.ExecuteBatch(b)
		if err != nil {
			return err
		}

		// calculate item and total amount
		if err = session.Query(`SELECT price FROM items WHERE item_id = ? LIMIT 1`, itemNumber).WithContext(ctx).Consistency(gocql.Quorum).Scan(&itemPrice); err != nil {
			log.Printf("Find item error: %v\n", err)
			return err
		}
		itemAmount := float32(quantity) * itemPrice
		//itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
		totalAmount += itemAmount

		orderLine := cassandra.OrderLine{
			OrderLineId:       idx + 1,
			ItemId:            itemNumber,
			SupplyWarehouseId: wId,
			Quantity:          quantity,
			Amount:            itemAmount,
			MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
		}
		orderLines = append(orderLines, orderLine)
	}

	if err = session.Query(`INSERT INTO orders (warehouse_id, district_id, customer_id, customer_id, carrier_id, items_number, status, entry_time, order_lines, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		warehouseId, districtId, customerId, total, local, time.Now(), orderLines, totalAmount).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - discount)
	return nil
}
