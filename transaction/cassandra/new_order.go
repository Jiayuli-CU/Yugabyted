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

	var warehouseTax, districtTax, discount float32
	var totalAmountInt int
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
		err = session.Query(`SELECT warehouse_tax_rate, district_tax_rate, next_order_number FROM cs5424_groupI.districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).WithContext(ctx).Consistency(gocql.Quorum).
			Scan(&warehouseTax, &districtTax, &orderId)
		if err != nil {
			log.Printf("Find district error: %v\n", err)
			continue
		}

		err = session.Query(`UPDATE cs5424_groupI.districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, orderId+1, warehouseId, districtId, orderId).
			WithContext(ctx).Exec()
		if err == nil {
			break
		}
	}

	if err = session.Query(`SELECT discount_rate FROM cs5424_groupI.customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
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
		//b.Entries = append(b.Entries, gocql.BatchEntry{
		//	Stmt:       "UPDATE cs5424_groupI.stock_counters SET quantity = quantity + 100 WHERE warehouse_id = ? AND item_id = ? IF quantity < 10",
		//	Args:       []interface{}{warehouseId, itemNumber},
		//	Idempotent: false,
		//})
		err = session.ExecuteBatch(b)
		if err != nil {
			return err
		}
		var stockQuantity int
		if err = session.Query(`SELECT quantity FROM cs5424_groupI.stock_counters WHERE warehouse_id = ? AND item_id = ?`, warehouseId, itemNumber).Scan(&stockQuantity); err != nil {
			log.Printf("Find quantity error: %v\n", err)
			return err
		}
		if stockQuantity < 10 {
			if err = session.Query(`UPDATE cs5424_groupI.stock_counters SET quantity = quantity + 100 WHERE warehouse_id = ? AND item_id = ?`, warehouseId, itemNumber).Exec(); err != nil {
				log.Printf("Find quantity error: %v\n", err)
				return err
			}
		}

		// calculate item and total amount
		if err = session.Query(`SELECT item_price FROM cs5424_groupI.items WHERE item_id = ? LIMIT 1`, itemNumber).WithContext(ctx).Consistency(gocql.Quorum).Scan(&itemPrice); err != nil {
			log.Printf("Find item error: %v\n", err)
			return err
		}
		itemAmountInt := quantity * int(itemPrice*100)
		//itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
		totalAmountInt += itemAmountInt

		orderLine := cassandra.OrderLine{
			OrderLineId:       idx + 1,
			ItemId:            itemNumber,
			SupplyWarehouseId: wId,
			Quantity:          quantity,
			AmountInt:         itemAmountInt,
			MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
		}
		orderLines = append(orderLines, orderLine)
	}

	if err = session.Query(`INSERT INTO cs5424_groupI.orders (warehouse_id, district_id, order_id, customer_id, items_number, all_local, entry_time, order_lines, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		warehouseId, districtId, orderId, customerId, total, local, time.Now(), orderLines, totalAmountInt).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	if err = session.Query(`UPDATE cs5424_groupI.customers SET last_order_id = ? WHERE warehouse_id =? AND district_id = ? AND customer_id = ?`, orderId, warehouseId, districtId, customerId).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(totalAmountInt, warehouseTax, districtTax, discount)
	totalAmount := float32(totalAmountInt) / 100 * (1 + warehouseTax + districtTax) * (1 - discount)
	fmt.Printf("%.2f", totalAmount)
	return nil
}
