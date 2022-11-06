package postgre

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"gorm.io/gorm"
	"log"
	"sync"
	"time"
)

func DeliveryTransactionV1(warehouseId, carrierId uint64) error {
	db := postgre.GetDB(false)
	err := db.Transaction(func(tx *gorm.DB) error {
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
		for districtId := 1; districtId <= 10; districtId++ {
			var order models.Order
			if err := tx.Model(&models.Order{}).Where("carrier_id = 0 AND warehouse_id = ? AND district_id = ?", warehouseId, districtId).First(&order).Error; err != nil {
				log.Printf("First order error: %v\n", err)
				return err
			}
			var customer models.Customer
			if err := tx.Model(&models.Customer{}).Where("id = ? AND warehouse_id = ? AND district_id = ?", order.CustomerId, order.WarehouseId, order.DistrictId).Find(&customer).Error; err != nil {
				log.Printf("Find customer error: %v\n", err)
				return err
			}
			order.CarrierId = carrierId
			if err := tx.Model(&models.Order{}).Where("id = ? AND warehouse_id = ? AND district_id = ?", order.Id, order.WarehouseId, order.DistrictId).Updates(&order).Error; err != nil {
				log.Printf("Update order error: %v\n", err)
				return err
			}
			var orderLines []models.OrderLine
			if err := tx.Model(&models.OrderLine{}).Where("warehouse_id = ? AND district_id = ? AND order_id = ?", order.WarehouseId, order.DistrictId, order.Id).Find(&orderLines).Error; err != nil {
				log.Printf("Find order lines error: %v\n", err)
				return err
			}
			totalAmount := 0.0
			for _, orderLine := range orderLines {
				orderLine.DeliveryTime = time.Now()
				totalAmount += orderLine.Price
				if err := tx.Model(&models.OrderLine{}).Where("warehouse_id = ? AND district_id = ? AND order_id = ? AND id = ?", orderLine.WarehouseId, orderLine.DistrictId, orderLine.OrderId, orderLine.Id).Updates(&orderLine).Error; err != nil {
					log.Printf("Update order line error: %v\n", err)
					return err
				}
			}
			customer.Balance += totalAmount
			customer.DeliveriesNumber++
			if err := tx.Model(&models.Customer{}).Where("warehouse_id = ? AND district_id = ? AND id = ?", customer.WarehouseId, customer.DistrictId, customer.Id).Updates(&customer).Error; err != nil {
				log.Printf("Update customer error: %v\n", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("Delivery transaction error: %v\n", err)
	}
	return err
}

func DeliveryTransaction(warehouseId, carrierId uint64) error {
	db := postgre.GetDB(false)
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
	var wg sync.WaitGroup
	for districtId := 1; districtId <= 10; districtId++ {
		wg.Add(1)
		go func(districtId int) {
			defer wg.Done()
			err = db.Transaction(func(tx *gorm.DB) error {
				var order models.Order
				if err = tx.Model(&models.Order{}).Where("carrier_id = 0 AND warehouse_id = ? AND district_id = ?", warehouseId, districtId).First(&order).Error; err != nil {
					log.Printf("First order error: %v\n", err)
					return err
				}
				var customer models.Customer
				if err = tx.Model(&models.Customer{}).Where("id = ? AND warehouse_id = ? AND district_id = ?", order.CustomerId, order.WarehouseId, order.DistrictId).Find(&customer).Error; err != nil {
					log.Printf("Find customer error: %v\n", err)
					return err
				}
				if err = tx.Model(&models.Order{}).Where("id = ? AND warehouse_id = ? AND district_id = ?", order.Id, order.WarehouseId, order.DistrictId).Updates(map[string]interface{}{"carrier_id": carrierId}).Error; err != nil {
					log.Printf("Update order error: %v\n", err)
					return err
				}
				var orderLines []models.OrderLine
				if err = tx.Model(&models.OrderLine{}).Where("warehouse_id = ? AND district_id = ? AND order_id = ?", order.WarehouseId, order.DistrictId, order.Id).Find(&orderLines).Error; err != nil {
					log.Printf("Find order lines error: %v\n", err)
					return err
				}
				totalAmount := 0.0
				for _, orderLine := range orderLines {
					totalAmount += orderLine.Price
					if err = tx.Model(&models.OrderLine{}).Where("id = ? AND warehouse_id = ? AND district_id = ? AND order_id = ?", orderLine.Id, orderLine.WarehouseId, orderLine.DistrictId, orderLine.OrderId).Set("delivery_time = ?", time.Now()).Error; err != nil {
						log.Printf("Update order line error: %v\n", err)
						return err
					}
				}
				if err = tx.Model(&models.Customer{}).Where("warehouse_id = ? AND district_id = ? AND id = ?", customer.WarehouseId, customer.DistrictId, customer.Id).Updates(map[string]interface{}{"balance": customer.Balance + totalAmount, "deliveries_number": customer.DeliveriesNumber + 1}).Error; err != nil {
					log.Printf("Update customer error: %v\n", err)
					return err
				}
				return nil
			})
			if err != nil {
				log.Printf("Delivery transaction error: %v\n", err)
				return
			}
		}(districtId)
	}

	wg.Wait()

	return err
}

func DeliveryTransactionV2(warehouseId, carrierId uint64) error {
	db := postgre.GetDB(false)
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
	err := db.Transaction(func(tx *gorm.DB) error {
		var orders []models.Order
		subQuery := tx.Table("orders").Select("district_id, min(id)").Where("carrier_id = 0 and warehouse_id = 1").Group("district_id")
		if err := tx.Table("orders").Select("id, customer_id, district_id").Where("warehouse_id = 1 and (district_id, id) in (?)", subQuery).Find(&orders).Error; err != nil {
			log.Printf("find orders err: %v\n", err)
			return err
		}

		var IdPairs [][]uint64
		for _, order := range orders {
			IdPairs = append(IdPairs, []uint64{order.DistrictId, order.CustomerId})
		}
		var customers []models.Customer
		if err := tx.Table("customers").Where("(district_id, id) in ? and warehouse_id = ?", IdPairs, warehouseId).Find(&customers).Error; err != nil {
			log.Printf("find customers err: %v\n", err)
			return err
		}
		return nil
	})

	return err
}
