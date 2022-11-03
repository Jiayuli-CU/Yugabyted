package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"encoding/csv"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
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
	b := session.NewBatch(gocql.CounterBatch)

	for districtId := 1; districtId <= 10; districtId++ {
		deliveryOrderId := 0

		err = session.Query(`SELECT next_delivery_order_id FROM cs5424_groupI.districts WHERE warehouse_id = ? AND district_id = ? LIMIT 1`, warehouseId, districtId).
			WithContext(ctx).Scan(&deliveryOrderId)
		if err != nil {
			log.Printf("Find district error: %v\n", err)
			return err
		}

		//CAS
		for {
			applied, err := session.Query(`UPDATE cs5424_groupI.districts SET next_order_number = ? WHERE warehouse_id = ? AND district_id = ? IF next_order_number = ?`, deliveryOrderId+1, warehouseId, districtId, deliveryOrderId).
				WithContext(ctx).ScanCAS(nil, nil, &deliveryOrderId)
			if applied && err == nil {
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
			WithContext(ctx).Scan(&customerId, &totalAmountInt); err != nil {
			log.Printf("Find order error: %v\n", err)
			return err
		}

		if customerId == 0 {
			go func() {
				key := fmt.Sprintf("%v:%v:%v", warehouseId, i+1, customerId)
				writeCSV(key, []string{
					fmt.Sprintf("%v", warehouseId),
					fmt.Sprintf("%v", i+1),
					fmt.Sprintf("%v", customerId),
					fmt.Sprintf("%v", orderId),
				})
			}()
		}

		if err = session.Query(`UPDATE cs5424_groupI.customer_counters SET balance = balance + ?, delivery_count = delivery_count + ? 
                                       WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?`,
			totalAmountInt, 1, warehouseId, i+1, customerId).
			WithContext(ctx).Exec(); err != nil {
			log.Printf("Update customer counter error: %v\n", err)
			return err
		}
	}

	return nil
}

func writeCSV(key string, output []string) {

	path := fmt.Sprintf("output_test/%v", key)
	csvFile, err := os.Create(path)
	if err != nil {
		log.Println("fail to open file")
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	err = writer.Write(output)
	if err != nil {
		log.Println(err)
	}

	writer.Flush()
}
