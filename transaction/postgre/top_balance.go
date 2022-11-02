package postgre

import (
	"cs5424project/store/models"
	"fmt"
	"log"

	"gorm.io/gorm"
)

func Top10Balance() error {

	var customers []models.Customer
	var balances []int
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Limit(10).Order("balance desc").Find(&customers).Error; err != nil {
			return err
		}
		for _, customer := range customers {
			curr_balance := customer.Balance
			balances = append(balances, int(curr_balance))
		}
		return nil
	})

	if err != nil {
		log.Printf("Top balance transaction error: %v", err)
		return err
	}

	for i, customer := range customers {
		customerFirst, customerMiddle, customerLast := customer.FirstName, customer.MiddleName, customer.LastName
		districtId, warehouseId := customer.DistrictId, customer.WarehouseId
		district := &models.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		if err := db.First(district).Error; err != nil {
			return err
		}

		warehouse := &models.Warehouse{
			Id: warehouseId,
		}
		if err := db.First(warehouse).Error; err != nil {
			return err
		}
		fmt.Printf("Customer name: First name: %v, Middle name: %v, Last name: %v\n", customerFirst, customerMiddle, customerLast)
		fmt.Printf("Balance amount: %v\n", balances[i])
		fmt.Printf("Warehouse name of the customer: %v\n", warehouse.Name)
		fmt.Printf("Distric name of the customer: %v\n", district.Name)
	}
	return err
}

func Top10Balance2() error {

	var customers []models.Customer

	err := db.Limit(10).Order("balance desc").Find(&customers).Error

	if err != nil {
		log.Printf("Top balance transaction error: %v", err)
	}
	return err
}
