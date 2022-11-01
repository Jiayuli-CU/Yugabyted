package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
)

func PaymentTransaction(ctx context.Context, warehouseId, districtId, customerId int, payment float32) error {
	// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
	// 		• Decrement C_BALANCE by PAYMENT
	// 		• Increment C_YTD_PAYMENT by PAYMENT
	// 		• Increment C_PAYMENT_CNT by 1

	paymentInt := int(payment * 100)
	var err error

	customerBasicInfo := cassandra.CustomerInfo{}
	var discount float32
	var balanceInt int

	if err = session.Query(`SELECT basic_info, discount_rate FROM cs5424_groupI.customers WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?`, warehouseId, districtId, customerId).
		WithContext(ctx).Scan(&customerBasicInfo, &discount); err != nil {
		log.Printf("Find customer error: %v\n", err)
		return err
	}

	if err = session.Query(`SELECT balance FROM cs5424_groupI.customer_counters WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?`, warehouseId, districtId, customerId).
		WithContext(ctx).Scan(&balanceInt); err != nil {
		log.Printf("Find customer counter error: %v\n", err)
		return err
	}

	customerInfo := CustomerInfoForPayment{
		CustomerIdentifier: CustomerIdentifier{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			CustomerId:  customerId,
		},
		CustomerBasicInfo: customerBasicInfo,
		Discount:          discount,
		Balance:           float32(balanceInt) / 100,
	}

	warehouseAddress := cassandra.WarehouseBasicInfo{}
	districtInfoAddress := cassandra.DistrictInfo{}

	if err = session.Query(`SELECT district_address, warehouse_address FROM cs5424_groupI.districts WHERE warehouse_id = ? AND district_id = ?`, warehouseId, districtId).
		WithContext(ctx).Scan(&warehouseAddress, &districtInfoAddress); err != nil {
		log.Printf("Find district counter error: %v\n", err)
		return err
	}

	if err = session.Query(`UPDATE cs5424_groupI.customer_counters SET payment_count = payment_count + ?, balance = balance - ?, year_to_date_payment = year_to_date_payment + ? WHERE warehouse_id = ? AND district_id = ? AND customer_id = ?`, 1, paymentInt, paymentInt, warehouseId, districtId, customerId).
		WithContext(ctx).Exec(); err != nil {
		log.Printf("Update customer counter error: %v\n", err)
		return err
	}

	// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
	if err = session.Query(`UPDATE cs5424_groupI.warehouse_counter SET warehouse_year_to_date_payment = warehouse_year_to_date_payment + ? WHERE warehouse_id = ?`, paymentInt, warehouseId).
		WithContext(ctx).Exec(); err != nil {
		log.Printf("Update warehouse counter error: %v\n", err)
		return err
	}

	if err = session.Query(`UPDATE cs5424_groupI.district_counter SET district_year_to_date_payment = district_year_to_date_payment + ? WHERE warehouse_id = ? AND district_id = ?`, paymentInt, warehouseId, districtId).
		Exec(); err != nil {
		log.Printf("Update district counter error: %v\n", err)
		return err
	}

	output := PaymentTransactionOutput{
		TransactionType:  "Payment Transaction",
		CustomerInfo:     customerInfo,
		WarehouseAddress: warehouseAddress,
		DistrictAddress:  districtInfoAddress,
		Payment:          payment,
	}

	fmt.Printf("%+v\n", output)
	println()

	return nil
}
