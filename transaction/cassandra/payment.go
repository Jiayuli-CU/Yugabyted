package cassandra

import (
	"log"
)

func PaymentTransaction(warehouseId, districtId, customerId int, payment float32) error {
	// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
	// 		• Decrement C_BALANCE by PAYMENT
	// 		• Increment C_YTD_PAYMENT by PAYMENT
	// 		• Increment C_PAYMENT_CNT by 1

	paymentInt := int(payment * 100)
	var err error

	if err = session.Query(`UPDATE cs5424_groupI.customer_counters SET payment_count = payment_count + ?, balance = balance - ?, year_to_date_payment = year_to_date_payment + ? WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?`, 1, paymentInt, paymentInt, warehouseId, districtId, customerId).
		Exec(); err != nil {
		log.Printf("Update customer counter error: %v\n", err)
		return err
	}

	// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT

	if err = session.Query(`UPDATE cs5424_groupI.warehouse_counter SET warehouse_year_to_date_payment = warehouse_year_to_date_payment + ? WHERE warehouse_id = ?`, paymentInt, warehouseId).
		Exec(); err != nil {
		log.Printf("Update warehouse counter error: %v\n", err)
		return err
	}

	if err = session.Query(`UPDATE cs5424_groupI.district_counter SET district_year_to_date_payment = district_year_to_date_payment + ? WHERE warehouse_id = ? AND district_id = ?`, paymentInt, warehouseId, districtId).
		Exec(); err != nil {
		log.Printf("Update district counter error: %v\n", err)
		return err
	}

	return nil
}
