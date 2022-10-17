package cassandra

import (
	"cs5424project/store/cassandra"
	"cs5424project/store/models"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

var session = cassandra.GetSession()

func DeliveryTransaction(warehouseId, carrierId uint64) error {
	// 1. For DISTRICT_NO = 1 to 10
	// 		(a) Let N denote the value of the smallest order number O_ID for district (W_ID,DISTRICT_NO)
	//			with O_CARRIER_ID = null; i.e.,
	//			N = min{t.O_ID ∈ Order | t.O_W_ID = W_ID, t.D_ID = DISTRICT_NO, t.O_CARRIER_ID = null}
	//			Let X denote the order corresponding to order number N, and let C denote the customer
	//			who placed this order
	//		(b) Update the order X by setting O_CARRIER_ID to CARRIER_ID
	//		(c) Update all the order-lines in X by setting OL_DELIVERY_D to the current date and time
	//		(d) Update customer C as follows:
	//			• Increment C_BALANCE by B, where B denote the sum of OL_AMOUNT for all the
	//			  items placed in order X
	//			• Increment C_DELIVERY_CNT by 1
	for districtId := 1; districtId <= 10; districtId++ {
		var order models.Order
		if err := session.Query(fmt.Sprintf(`SELECT * FROM orders WHERE carrier_id = null AND warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)).
			Consistency(gocql.Quorum).Scan(&order); err != nil {
			log.Printf("First order error: %v\n", err)
			return err
		}
		var customer models.Customer
		if err := session.Query(fmt.Sprintf(`SELECT * FROM customers WHERE id = %v AND warehouse_id = %v AND district_id = %v`, order.CustomerId, order.WarehouseId, order.DistrictId)).
			Consistency(gocql.Quorum).Scan(&customer); err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		if err := session.Query(fmt.Sprintf(`UPDATE customers SET carrier_id = %v WHERE id = %v AND warehouse_id = %v AND district_id = %v`, carrierId, customer.Id, customer.WarehouseId, customer.DistrictId)).
			Consistency(gocql.Quorum).Exec(); err != nil {
			log.Printf("Update order error: %v\n", err)
			return err
		}
		var orderLines []models.OrderLine
		if err := session.Query(fmt.Sprintf(`SELECT * FROM orderlines WHERE warehouse_id = %v AND district_id = %v AND order_id = %v`, order.WarehouseId, order.DistrictId, order.Id)).
			Consistency(gocql.Quorum).Scan(&orderLines); err != nil {
			log.Printf("Find order lines error: %v\n", err)
			return err
		}
		totalAmount := customer.Balance
		for _, orderLine := range orderLines {
			orderLine.DeliveryTime = time.Now()
			totalAmount += orderLine.Price
			if err := session.Query(fmt.Sprintf(`UPDATE orderlines SET delivery_time = %v WHERE warehouse_id = %v AND district_id = %v AND order_id = %v AND id = %v`, time.Now(), orderLine.WarehouseId, orderLine.DistrictId, orderLine.OrderId, orderLine.Id)).
				Consistency(gocql.Quorum).Exec(); err != nil {
				log.Printf("Update order line error: %v\n", err)
				return err
			}
		}
		deliveryNumber := customer.DeliveriesNumber + 1
		if err := session.Query(fmt.Sprintf(`UPDATE customers SET balance = %v, deliveries_number = %v WHERE warehouse_id = %v AND district_id = %v AND id = %v`, totalAmount, deliveryNumber, customer.WarehouseId, customer.DistrictId, customer.Id)).
			Consistency(gocql.Quorum).Exec(); err != nil {
			log.Printf("Update customer error: %v\n", err)
			return err
		}
	}
	return nil
}
