package cassandra

import (
	"context"
	"cs5424project/store/models"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

func NewOrder(warehouseId, districtId, customerId, total uint64, itemNumbers, supplierWarehouses []uint64, quantities []int) error {

	var warehouseTax, districtTax, discount, totalAmount float64
	var warehouse *models.Warehouse
	var customer *models.Customer
	var district *models.District
	var err error

	local := 1
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = 0
			break
		}
	}

	ctx := context.Background()

	if err = session.Query(`SELECT * FROM warehouses WHERE id = ? LIMIT 1`, warehouseId).WithContext(ctx).Consistency(gocql.Quorum).Scan(warehouse); err != nil {
		log.Printf("Find warehouse error: %v\n", err)
		return err
	}
	warehouseTax = warehouse.TaxRate

	if err = session.Query(`SELECT * FROM districts WHERE id = ? LIMIT 1`, districtId).WithContext(ctx).Consistency(gocql.Quorum).Scan(district); err != nil {
		log.Printf("Find district error: %v\n", err)
		return err
	}
	orderId := district.NextAvailableOrderNumber
	districtTax = district.TaxRate
	if err = session.Query(`UPDATE districts SET next_available_order_number = ? WHERE id = ?`, orderId+1, districtId).
		WithContext(ctx).Exec(); err != nil {
		log.Printf("Update next_available_order_number failed: %v\n", err)
		return err
	}

	if err = session.Query(`SELECT * FROM customers WHERE id = ? AND warehouse_id = ? AND district_id = ? LIMIT 1`, customerId, warehouseId, districtId).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(customer); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}
	discount = customer.DiscountRate

	//create new order
	newOrder := &models.Order{
		Id:          orderId,
		DistrictId:  districtId,
		WarehouseId: warehouseId,
		CustomerId:  customerId,
		EntryTime:   time.Now(),
		ItemsNumber: total,
		Status:      local,
	}

	if err = session.Query(`INSERT INTO orders (id, warehouse_id, district_id, customer_id, carrier_id, items_number, status, entry_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		newOrder.Id, newOrder.WarehouseId, newOrder.DistrictId, newOrder.CustomerId, newOrder.CarrierId, newOrder.ItemsNumber, newOrder.Status, newOrder.EntryTime).
		WithContext(ctx).Exec(); err != nil {
		log.Fatal(err)
	}

	for idx, itemNumber := range itemNumbers {

		wId := supplierWarehouses[idx]
		quantity := quantities[idx]

		stock := &models.Stock{}
		if err = session.Query(`SELECT * FROM stocks WHERE warehouse_id = ? AND item_id = ? LIMIT 1`, wId, itemNumber).WithContext(ctx).Consistency(gocql.Quorum).Scan(stock); err != nil {
			log.Printf("Find district error: %v\n", err)
			return err
		}

		// update stock
		stockQuantity := stock.Quantity
		adjustedQuantity := stockQuantity - quantity
		if adjustedQuantity < 10 {
			adjustedQuantity += 100
		}
		stock.Quantity = adjustedQuantity
		stock.OrdersNumber += 1
		if wId != warehouseId {
			stock.RemoteOrdersNumber += 1
		}
		stock.YearToDateQuantityOrdered += quantity
		// 此处更新有无更好办法？
		if err = session.Query(`UPDATE stocks SET quantity = ? AND orders_number = ? AND remote_orders_number = ? WHERE warehouse_id = ? AND item_id = ?`, stock.Quantity, stock.OrdersNumber, stock.RemoteOrdersNumber, wId, itemNumber).
			WithContext(ctx).Exec(); err != nil {
			log.Printf("Update stock failed: %v\n", err)
			return err
		}

		// calculate item and total amount
		item := &models.Item{}
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
