package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
	"time"
)

func OrderStatusTransaction(ctx context.Context, warehouseId, districtId, customerId int) error {

	var err error
	var customerInfo cassandra.CustomerInfo
	var balanceInt, lastOrderId int
	if err = session.Query(`SELECT basic_info, last_order_id FROM cs5424_groupI.customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		WithContext(ctx).Scan(&customerInfo, &lastOrderId); err != nil {
		log.Printf("Find customer basic info error: %v\n", err)
		return err
	}
	if err = session.Query(`SELECT balance FROM cs5424_groupI.customer_counters WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		WithContext(ctx).Scan(&balanceInt); err != nil {
		log.Printf("Find customer balance error: %v\n", err)
		return err
	}

	var entryTime, deliveryTime time.Time
	var carrierId int
	var orderLines []cassandra.OrderLine

	if err = session.Query(`SELECT entry_time, carrier_id, order_lines, delivery_time FROM cs5424_groupI.orders WHERE warehouse_id = ? AND district_id = ? AND order_id = ? LIMIT 1`, warehouseId, districtId, lastOrderId).
		WithContext(ctx).Scan(&entryTime, &carrierId, &orderLines, &deliveryTime); err != nil {
		log.Printf("Find order error: %v\n", err)
		return err
	}

	items := make([]OrderStatusItemInfo, len(orderLines))
	for i, orderLine := range orderLines {
		item := OrderStatusItemInfo{
			ItemId:              orderLine.ItemId,
			SupplierWarehouseId: warehouseId,
			Quantity:            orderLine.Quantity,
			Amount:              float32(orderLine.AmountInt) / 100,
			DeliveryDate:        deliveryTime,
		}

		items[i] = item
	}

	output := OrderStatusTransactionOutput{
		TransactionType: "Order Status Transaction",
		FirstName:       customerInfo.FirstName,
		MiddleName:      customerInfo.MiddleName,
		LastName:        customerInfo.LastName,
		Balance:         float32(balanceInt) / 100,
		LastOrderId:     lastOrderId,
		EntryDate:       entryTime,
		CarrierId:       carrierId,
		Items:           items,
	}

	fmt.Printf("%+v\n", output)
	println()
	return nil
}
