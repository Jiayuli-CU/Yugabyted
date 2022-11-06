package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"github.com/gocql/gocql"
	"time"
)

var session = cassandra.GetSession()

func DeliveryTransaction(ctx context.Context, warehouseId, carrierId int) error {
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
	b := session.NewBatch(gocql.UnloggedBatch)

	districts := make([]int, 10)
	for i := 0; i < 10; i++ {
		districts[i] = i + 1
	}

	scanner := session.Query(`SELECT next_order_number, next_delivery_order_id FROM cs5424_groupl.districts WHERE warehouse_id = ? AND district_id IN ?`, warehouseId, districts).Iter().Scanner()
	districtId := 1
	for scanner.Next() {
		var nextOrder int
		scanner.Scan(&nextOrder, &deliveryOrderIds[districtId-1])
		if nextOrder <= deliveryOrderIds[districtId-1] {
			//fmt.Printf("Delivery Transaction Failed: warehouseId: %v, districtId: %v, next order id: %v, next deliveryOrderId: %v\n", warehouseId, districtId, nextOrder, deliveryOrderIds[districtId-1])
			//return errors.Errorf("all orders has been delivered for some districts")
			deliveryOrderIds[districtId-1] = 0
		}
		districtId += 1
	}

	for i, deliveryOrderId := range deliveryOrderIds {
		if deliveryOrderId == 0 {
			continue
		}
		applied, err := session.Query(`UPDATE cs5424_groupl.districts SET next_delivery_order_id = ? WHERE warehouse_id = ? AND district_id = ? IF next_delivery_order_id = ?`, deliveryOrderId+1, warehouseId, i+1, deliveryOrderId).
			WithContext(ctx).ScanCAS(nil, nil, &deliveryOrderId)
		if !applied || err != nil {
			deliveryOrderIds[i] = 0
			continue
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE cs5424_groupl.orders SET carrier_id = ?, delivery_time = ? WHERE warehouse_id = ? AND district_id = ? AND order_id = ?",
			Args:       []interface{}{carrierId, time.Now(), warehouseId, i + 1, deliveryOrderId},
			Idempotent: true,
		})
	}

	err = session.ExecuteBatch(b)
	if err != nil {
		return err
	}

	var totalAmountInt int
	var customerId int

	b = session.NewBatch(gocql.CounterBatch)

	for i, deliveryOrderId := range deliveryOrderIds {
		if deliveryOrderId == 0 {
			continue
		}
		err = session.Query(`SELECT customer_id, total_amount FROM cs5424_groupl.orders WHERE warehouse_id = ? AND district_id = ? AND order_id = ?`, warehouseId, i+1, deliveryOrderId).
			WithContext(ctx).Scan(&customerId, &totalAmountInt)
		if err != nil {
			return err
		}

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "UPDATE cs5424_groupl.customer_counters SET balance = balance + ?, delivery_count = delivery_count + ? WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?",
			Args:       []interface{}{totalAmountInt, 1, warehouseId, i + 1, customerId},
			Idempotent: false,
		})
	}

	err = session.ExecuteBatch(b)
	return err

}
