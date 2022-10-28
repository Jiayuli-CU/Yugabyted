package cassandra

import (
	"cs5424project/store/cassandra"
	"github.com/gocql/gocql"
	"log"
	"time"
)

var session = cassandra.GetSession()

func DeliveryTransaction(warehouseId, carrierId int) error {
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

	var err error

	deliveryOrderIds := make([]int, 10)
	b := session.NewBatch(gocql.CounterBatch)

	for districtId := 1; districtId <= 1; districtId++ {
		deliveryOrderId := 0
		for {
			err = session.Query(`SELECT next_delivery_order_id FROM cs5424_groupI.districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).Consistency(gocql.Quorum).
				Scan(&deliveryOrderId)
			if err != nil {
				log.Printf("Find district error: %v\n", err)
				continue
			}

			err = session.Query(`UPDATE cs5424_groupI.districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, deliveryOrderId+1, warehouseId, districtId, deliveryOrderId).
				Exec()
			if err == nil {
				deliveryOrderIds[districtId-1] = deliveryOrderId
				break
			}
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE cs5424_groupI.orders SET carrier_id = ?, delivery_time = ? WHERE warehouse_id = ? AND district_id = ? AND order_id = ?",
			Args:       []interface{}{carrierId, time.Now(), warehouseId, districtId, deliveryOrderId},
			Idempotent: true,
		})
	}

	err = session.ExecuteBatch(b)
	if err != nil {
		return err
	}

	var totalAmountInt int
	var customerId int

	for i, orderId := range deliveryOrderIds {
		if err = session.Query(`SELECT customer_id, total_amount FROM cs5424_groupI.orders WHERE warehouse_id = ? AND district_id = ? AND order_id = ?`, warehouseId, i+1, orderId).
			Scan(&customerId, &totalAmountInt); err != nil {
			log.Printf("Find order error: %v\n", err)
			return err
		}

		if err = session.Query(`UPDATE cs5424_groupI.customer_counters SET balance = balance + ?, delivery_count = delivery_count + ?`, totalAmountInt, 1).Exec(); err != nil {
			log.Printf("Update customer error: %v\n", err)
			return err
		}
	}

	return nil
}
