package cassandra

import (
	"cs5424project/store/cassandra"
	"log"
	"time"
)

func OrderStatusTransaction(warehouseId, districtId, customerId int) error {

	var err error
	var customerInfo cassandra.CustomerInfo
	var balanceInt, lastOrderId int
	if err = session.Query(`SELECT basic_info, last_order_id FROM cs5424_groupI.customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		Scan(&customerInfo, &lastOrderId); err != nil {
		log.Printf("Find customer basic info error: %v\n", err)
		return err
	}
	if err = session.Query(`SELECT balance FROM cs5424_groupI.customer_counters WHERE warehouse_id = ? AND district_id = ? AND customer_id = ? LIMIT 1`, warehouseId, districtId, customerId).
		Scan(&balanceInt); err != nil {
		log.Printf("Find customer balance error: %v\n", err)
		return err
	}

	var entryTime, deliveryTime time.Time
	var carrierId int
	var orderLines []cassandra.OrderLine

	if err = session.Query(`SELECT entry_time, carrier_id, order_lines, delivery_time FROM cs5424_groupI.orders WHERE warehouse_id = ? AND district_id = ? AND order_id = ? LIMIT 1`, warehouseId, districtId, lastOrderId).
		Scan(&entryTime, &carrierId, &orderLines, &deliveryTime); err != nil {
		log.Printf("Find order error: %v\n", err)
		return err
	}

	log.Printf("Customer info: first name = %v, middle name = %v, last name = %v, balance = %v\n",
		customerInfo.FirstName,
		customerInfo.MiddleName,
		customerInfo.LastName,
		balanceInt/100.0,
	)
	log.Printf("Customer last order info: order id = %v, entry time = %v, carrier id = %v",
		lastOrderId,
		entryTime,
		carrierId,
	)
	for _, orderLine := range orderLines {
		log.Printf("Customer order item info: order info = %v, warehouse id = %v, quantity ordered = %v, total price = %v, delivery time = %v\n",
			orderLine.ItemId,
			warehouseId,
			orderLine.Quantity,
			orderLine.AmountInt/100.0,
			deliveryTime,
		)
	}
	return nil
}
