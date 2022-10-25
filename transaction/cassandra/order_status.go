package cassandra

import (
	"cs5424project/store/postgre"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

func OrderStatusTransaction(warehouseId, districtId, customerId uint64) error {
	var customer postgre.Customer
	if err := session.Query(fmt.Sprintf(`SELECT * FROM customers WHERE id = %v AND warehouse_id = %v AND district_id = %v LIMIT 1`, customerId, warehouseId, districtId)).
		Consistency(gocql.Quorum).Scan(&customer); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}
	var order postgre.Order
	if err := session.Query(fmt.Sprintf(`SELECT * FROM orders WHERE customer_id = %v AND warehouse_id = %v AND district_id = %v ORDER BY id DESC LIMIT 1`, customer.Id, customer.WarehouseId, customer.DistrictId)).
		Consistency(gocql.Quorum).Scan(&order); err != nil {
		log.Printf("Last order error: %v\n", err)
		return err
	}
	var orderLines []postgre.OrderLine
	if err := session.Query(fmt.Sprintf(`SELECT * FROM orderlines WHERE order_id = %v AND warehouse_id = %v AND district_id = %v`, order.Id, order.WarehouseId, order.DistrictId)).
		Consistency(gocql.Quorum).Scan(&orderLines); err != nil {
		log.Printf("Find order lines error: %v\n", err)
		return err
	}
	log.Printf("Customer info: first name = %v, middle name = %v, last name = %v, balance = %v\n",
		customer.FirstName,
		customer.MiddleName,
		customer.LastName,
		customer.Balance,
	)
	log.Printf("Customer last order info: order id = %v, entry time = %v, carrier id = %v",
		order.Id,
		order.EntryTime,
		order.CarrierId,
	)
	for _, orderLine := range orderLines {
		log.Printf("Customer order item info: order info = %v, warehouse id = %v, quantity ordered = %v, total price = %v, delivery time = %v\n",
			orderLine.ItemId,
			orderLine.WarehouseId,
			orderLine.Quantity,
			orderLine.Price,
			orderLine.DeliveryTime,
		)
	}
	return nil
}
