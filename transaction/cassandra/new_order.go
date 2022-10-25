package cassandra

import (
	"context"
	"cs5424project/store/models"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"log"
	"time"
)

func NewOrder(warehouseId, districtId, customerId, total uint64, itemNumbers, supplierWarehouses []uint64, quantities []int) error {

	var warehouseTax, districtTax, discount, totalAmount float64
	var warehouse *models.Warehouse
	var customer *models.Customer
	var district *models.District
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

	if err = session.Query(`SELECT warehouse_tax_rate, district_tax_rate, next_order_number FROM districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).WithContext(ctx).Consistency(gocql.Quorum).Scan(&warehouseTax, &districtTax, &orderId); err != nil {
		log.Printf("Find district error: %v\n", err)
		return err
	}
	warehouseTax = warehouse.TaxRate

	//if err = session.Query(`SELECT * FROM districts WHERE id = ? LIMIT 1`, districtId).WithContext(ctx).Consistency(gocql.Quorum).Scan(district); err != nil {
	//	log.Printf("Find district error: %v\n", err)
	//	return err
	//}
	////orderId := district.NextAvailableOrderNumber
	//districtTax = district.TaxRate
	if err = session.Query(`UPDATE districts SET next_available_order_number = ? WHERE id = ?`, orderId+1, districtId).
		WithContext(ctx).Exec(); err != nil {
		log.Printf("Update next_available_order_number failed: %v\n", err)
		return err
	}

	//CAS to handle concurrent read and write
	//try 10 times
	i := 0
	for ; i < 10; i++ {
		err = session.Query(`UPDATE districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, orderId+1, warehouseId, districtId, orderId).
			WithContext(ctx).Exec()
		if err == nil {
			break
		}
		err = session.Query(`SELECT next_order_number FROM districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).WithContext(ctx).Consistency(gocql.Quorum).Scan(&orderId)
		if err != nil {
			log.Printf("Find district error: %v\n", err)
			return err
		}
	}

	if i == 10 {
		return errors.Errorf("fail to get and update order id")
	}

	if err = session.Query(`SELECT discount_rate FROM customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(&discount); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}
	discount = customer.DiscountRate

	//create new order
	newOrder := &models.Order{
		Id:          uint64(orderId),
		DistrictId:  districtId,
		WarehouseId: warehouseId,
		CustomerId:  customerId,
		EntryTime:   time.Now(),
		ItemsNumber: total,
		Status:      local,
	}

	//if err = session.Query(`INSERT INTO orders (id, warehouse_id, district_id, customer_id, carrier_id, items_number, status, entry_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
	//	newOrder.Id, newOrder.WarehouseId, newOrder.DistrictId, newOrder.CustomerId, newOrder.CarrierId, newOrder.ItemsNumber, newOrder.Status, newOrder.EntryTime).
	//	WithContext(ctx).Exec(); err != nil {
	//	log.Fatal(err)
	//}

	var stockQuantity int
	var itemPrice float64

	for idx, itemNumber := range itemNumbers {

		wId := supplierWarehouses[idx]
		quantity := quantities[idx]

		//stock := &models.Stock{}
		//if err = session.Query(`SELECT quantity FROM stock_counter WHERE warehouse_id = ? AND item_id = ? LIMIT 1`, wId, itemNumber).WithContext(ctx).Consistency(gocql.Quorum).Scan(&stockQuantity); err != nil {
		//	log.Printf("Find district error: %v\n", err)
		//	return err
		//}

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
		if err = session.Query(`SELECT * FROM items WHERE id = ? LIMIT 1`, itemNumber).WithContext(ctx).Consistency(gocql.Quorum).Scan(item); err != nil {
			log.Printf("Find item error: %v\n", err)
			return err
		}
		itemAmount := float64(quantity) * item.Price
		//itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
		totalAmount += itemAmount

		orderLine := &models.OrderLine{
			OrderId:           orderId,
			DistrictId:        districtId,
			WarehouseId:       warehouseId,
			Id:                uint64(idx + 1),
			ItemId:            itemNumber,
			SupplyNumber:      wId,
			Quantity:          quantity,
			Price:             itemAmount,
			MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
		}

		if err = session.Query(`INSERT INTO order_lines (id, warehouse_id, district_id, order_id, item_id, delivery_time, price, supply_number, quantity, miscellaneous_data) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			orderLine.Id, orderLine.WarehouseId, orderLine.DistrictId, orderLine.OrderId, orderLine.ItemId, orderLine.DeliveryTime, orderLine.Price, orderLine.SupplyNumber, orderLine.MiscellaneousData).
			WithContext(ctx).Exec(); err != nil {
			log.Fatal(err)
		}
	}

	totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - discount)
	return nil
}
