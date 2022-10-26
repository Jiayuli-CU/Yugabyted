package cassandra

import (
	"fmt"
	"log"
)

func PaymentTransaction(warehouseId, districtId, customerId uint64, payment float32) error {
	// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
	// 		• Decrement C_BALANCE by PAYMENT
	// 		• Increment C_YTD_PAYMENT by PAYMENT
	// 		• Increment C_PAYMENT_CNT by 1
	var balance, yearToDatePayment float32
	var paymentCount int
	var err error

	for {
		if err = session.Query(fmt.Sprintf(`SELECT payment_count, balance, year_to_date_payment FROM customers WHERE warehouse_id = %v AND district_id = %v AND customer_id = %v`, warehouseId, districtId, customerId)).
			Scan(&paymentCount, &balance, &yearToDatePayment); err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		balance -= payment
		yearToDatePayment += payment

		if err = session.Query(fmt.Sprintf(`UPDATE customers SET payment_count = %v, balance = %v, year_to_date_payment = %v WHERE warehouse_id = %v AND district_id = %v AND customer_id = %v IF payment_count = %v`, paymentCount+1, balance, yearToDatePayment, warehouseId, districtId, customerId, paymentCount)).
			Exec(); err != nil {
			log.Printf("Update customer error: %v\n", err)
		}

		if err == nil {
			break
		}
	}

	// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
	var warehouseYearToDatePayment, districtYearToDatePayment float32
	for {
		if err = session.Query(fmt.Sprintf(`SELECT warehouse_year_to_date_payment, district_year_to_date_payment FROM districts WHERE warehouse_id = %v AND district_id = %v`, warehouseId, districtId)).
			Scan(&warehouseYearToDatePayment, &districtYearToDatePayment); err != nil {
			log.Printf("Find district error: %v\n", err)
			continue
		}
		districtYearToDatePayment += payment

		if err = session.Query(fmt.Sprintf(`UPDATE districts SET warehouse_year_to_date_payment = %v, district_year_to_date_payment = %v WHERE warehouse_id = %v AND district_id = %v IF warehouse_year_to_date_payment = %v`, warehouseYearToDatePayment+payment, districtYearToDatePayment, warehouseId, districtId, warehouseYearToDatePayment)).
			Exec(); err != nil {
			log.Printf("Update district error: %v\n", err)
		}

		if err == nil {
			break
		}
	}

	return nil
}
