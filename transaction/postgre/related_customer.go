package postgre

import (
	"cs5424project/store/models"
	"gorm.io/gorm"
	"log"
)

func RelatedCustomerTransaction(customerId, warehouseId, districtId uint64) error {
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
		var customer models.Customer
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		var orders []models.Order
		if err := tx.Model(&models.Order{}).Where("customer_id = ? AND warehouse_id = ? AND district_id = ?", customer.Id, customer.WarehouseId, customer.DistrictId).Find(&orders).Error; err != nil {
			log.Printf("Find orders error: %v\n", err)
			return err
		}
		var allOrderLines []models.OrderLine
		for _, order := range orders {
			var orderLines []models.OrderLine
			if err := tx.Model(&models.OrderLine{}).Where("order_id = ? AND warehouse_id = ? AND district_id = ?", order.Id, order.WarehouseId, order.DistrictId).Find(&orderLines).Error; err != nil {
				log.Printf("Find order lines error: %v\n", err)
				return err
			}
			allOrderLines = append(allOrderLines, orderLines...)
		}
		//todo

		return nil
	})
	if err != nil {
		log.Printf("Related customer transaction error: %v", err)
	}
	return err
}
