package postgre

import (
	"cs5424project/store/models"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"log"
)

func PaymentTransaction(warehouseId, districtId, customerId uint64, payment float64) {
	err := db.Transaction(func(tx *gorm.DB) error {
		// 1. Update the customer (C_W_ID, C_D_ID, C_ID) as follows:
		// 		• Decrement C_BALANCE by PAYMENT
		// 		• Increment C_YTD_PAYMENT by PAYMENT
		// 		• Increment C_PAYMENT_CNT by 1
		var customer models.Customer
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		if customer.Balance < payment {
			return errors.New(fmt.Sprintf("Not enough balance. Current balance is %v, need to pay %v\n", customer.Balance, payment))
		}
		customer.Balance -= payment
		customer.YearToDatePayment += payment
		customer.PaymentsNumber++
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Updates(&customer).Error; err != nil {
			log.Printf("Update customer error: %v\n", err)
			return err
		}

		// 2. Update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
		var warehouse models.Warehouse
		if err := tx.Model(&models.Warehouse{}).
			Where("id = ?", warehouseId).
			Find(&warehouse).Error; err != nil {
			log.Printf("Find warehouse error: %v\n", err)
			return err
		}
		warehouse.YearToDateAmount += payment
		if err := tx.Model(&models.Warehouse{}).
			Where("id = ?", warehouseId).
			Updates(&warehouse).Error; err != nil {
			log.Printf("Update warehouse error: %v\n", err)
			return err
		}

		// 3. Update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
		var district models.District
		if err := tx.Model(&models.District{}).
			Where("id = ? AND warehouse_id = ?", districtId, warehouseId).
			Find(&district).Error; err != nil {
			log.Printf("Find district error: %v\n", err)
			return err
		}
		district.Year2DateAmount += payment
		if err := tx.Model(&models.District{}).
			Where("id = ? AND warehouse_id = ?", districtId, warehouseId).
			Updates(&district).Error; err != nil {
			log.Printf("Update district error: %v\n", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Payment transaction error: %v\n", err)
	}
}
