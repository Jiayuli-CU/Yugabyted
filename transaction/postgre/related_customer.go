package postgre

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"gorm.io/gorm"
	"log"
	"sync"
)

func RelatedCustomerTransaction(customerId, warehouseId, districtId uint64) error {
	var currCustomer models.Customer
	db := postgre.GetDB(false)
	err := db.Transaction(func(tx *gorm.DB) error {
		// 1. Let S be the set of customers who are related to the customer identified by (C_W_ID, C_D_ID,
		//	  C_ID).
		//	  S = {C′ ∈ Customer | C′.C_W_ID != C_W_ID,
		//	  ∃ O ∈ Order, O.O W_ID = C_W_ID, O.O_D_ID = C_D_ID, O.O_C_ID = C_ID,
		//	  ∃ O′ ∈ Order, O′.O W_ID = C′.C_W_ID, O′.O_D_ID = C′.C_D_ID, O′.O_C_ID = C′.C_ID
		//	  ∃ OL1 ∈ OrderItem, OL1.OL_W_ID = O.O_W_ID, OL1.OL_D_ID = O.O_D_ID, OL1.OL_O_ID = O.O_ID,
		//	  ∃ OL2 ∈ OrderItem, OL2.OL_W_ID = O.O_W_ID, OL2.OL_D_ID = O.O_D_ID, OL2.OL_O_ID = O.O_ID,
		//	  ∃ OL1′ ∈ OrderItem, OL1′.OL_W_ID = O′.O_W_ID, OL1′.OL_D_ID = O′.O_D_ID, OL1′.OL_O_ID = O′.O_ID,
		//	  ∃ OL2′ ∈ OrderItem, OL2′.OL_W_ID = O′.O_W_ID, OL2′.OL_D_ID = O′.O_D_ID, OL2′.OL_O_ID = O′.O_ID,
		//	  OL1.OL_I_ID != OL2.OL_I_ID, OL1′.OL_I_ID != OL2′.OL_I_ID,
		//	  OL1.OL_I_ID = OL1′.OL_I_ID, OL2.OL_I_ID = OL2′.OL_I_ID}
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Find(&currCustomer).Error; err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		return nil
	})

	currCustomerItemSet, err := getCustomerOrderItemsTransaction(db, currCustomer)
	if err == nil {
		var customers []models.Customer
		err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.Customer{}).Where("warehouse_id != ?", currCustomer.WarehouseId).
				Find(&customers).Error; err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Printf("Find all customers error: %v", err)
			return err
		}
		var wg sync.WaitGroup
		for _, customer := range customers {
			wg.Add(1)
			go func(customer models.Customer) {
				defer wg.Done()
				customerItemSet, err := getCustomerOrderItemsTransaction(db, customer)
				if err != nil {
					return
				}
				countCommon := 0
				for key, _ := range customerItemSet {
					if currCustomerItemSet[key] {
						countCommon++
					}
					if countCommon >= 2 {
						log.Printf("Related customer info: warehouse_id = %v, district_id = %v, customer_id = %v", customer.WarehouseId, customer.DistrictId, customer.Id)
						return
					}
				}
			}(customer)
		}
		wg.Wait()
	}

	return err
}

func getCustomerOrderItemsTransaction(db *gorm.DB, customer models.Customer) (map[uint64]bool, error) {
	var allOrderLines []models.OrderLine

	err := db.Transaction(func(tx *gorm.DB) error {
		var orders []models.Order
		if err := tx.Model(&models.Order{}).Where("warehouse_id = ? AND district_id = ? AND customer_id = ?", customer.WarehouseId, customer.DistrictId, customer.Id).Find(&orders).Error; err != nil {
			log.Printf("Find orders error: %v\n", err)
			return err
		}
		for _, order := range orders {
			var orderLines []models.OrderLine
			if err := tx.Model(&models.OrderLine{}).Where("order_id = ? AND warehouse_id = ? AND district_id = ?", order.Id, order.WarehouseId, order.DistrictId).Find(&orderLines).Error; err != nil {
				log.Printf("Find order lines error: %v\n", err)
				return err
			}
			allOrderLines = append(allOrderLines, orderLines...)
		}
		return nil
	})
	if err != nil {
		log.Printf("Related customer transaction error: %v", err)
		return nil, err
	}

	currCustomerItemSet := map[uint64]bool{}
	for _, orderLine := range allOrderLines {
		currCustomerItemSet[orderLine.ItemId] = true
	}

	return currCustomerItemSet, nil
}
