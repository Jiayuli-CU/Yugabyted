package cassandra

import (
	"cs5424project/store/postgre"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

func PaymentTransaction(warehouseId, districtId, customerId uint64, payment float64) error {
	// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
	// 		• Decrement C_BALANCE by PAYMENT
	// 		• Increment C_YTD_PAYMENT by PAYMENT
	// 		• Increment C_PAYMENT_CNT by 1
	var customer postgre.Customer
	if err := session.Query(fmt.Sprintf(`SELECT * FROM customers WHERE id = %v AND warehouse_id = %v AND district_id = %v`, customerId, warehouseId, districtId)).
		Consistency(gocql.Quorum).Scan(&customer); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}
	if customer.Balance < payment {
		return errors.New(fmt.Sprintf("Not enough balance. Current balance is %v, need to pay %v\n", customer.Balance, payment))
	}
	balance := customer.Balance - payment
	yearToDatePayment := customer.YearToDatePayment + payment
	paymentsNumber := customer.PaymentsNumber + 1
	if err := session.Query(fmt.Sprintf(`UPDATE customers SET balance = %v, year_to_date_payment = %v, payments_number = %v WHERE id = %v AND warehouse_id = %v AND district_id = %v`, balance, yearToDatePayment, paymentsNumber, customerId, warehouseId, districtId)).
		Consistency(gocql.Quorum).Exec(); err != nil {
		log.Printf("Update customer error: %v\n", err)
		return err
	}

	// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
	var warehouse postgre.Warehouse
	if err := session.Query(fmt.Sprintf(`SELECT * FROM warehouses WHERE id = %v`, warehouseId)).
		Consistency(gocql.Quorum).Scan(&warehouse); err != nil {
		log.Printf("Find warehouse error: %v\n", err)
		return err
	}
	yearToDateAmount := warehouse.YearToDateAmount + payment
	if err := session.Query(fmt.Sprintf(`UPDATE warehouses SET year_to_date_amount = %v WHERE id = %v`, yearToDateAmount, warehouseId)).
		Consistency(gocql.Quorum).Exec(); err != nil {
		log.Printf("Update warehouse error: %v\n", err)
		return err
	}

	// 3. Update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
	var district postgre.District
	if err := session.Query(fmt.Sprintf(`SELECT * FROM districts WHERE id = %v AND warehouse_id = %v`, district, warehouseId)).
		Consistency(gocql.Quorum).Scan(&district); err != nil {
		log.Printf("Find district error: %v\n", err)
		return err
	}
	yearToDateAmount = district.YearToDateAmount + payment
	if err := session.Query(fmt.Sprintf(`UPDATE districts SET year_to_date_amount = %v WHERE id = %v AND warehouse_id = %v`, yearToDateAmount, districtId, warehouseId)).
		Consistency(gocql.Quorum).Exec(); err != nil {
		log.Printf("Update district error: %v\n", err)
		return err
	}

	return nil
}
