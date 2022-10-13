package transaction

import (
	"cs5424project/store/models"
	"gorm.io/gorm"
	"log"
	"time"
)

func DeliveryTransaction(warehouseId, carrierId uint64) {
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
		var order models.Order
		if err := tx.Model(&models.Order{}).Where("carrier_id = null").First(&order).Error; err != nil {
			log.Printf("First order error: %v\n", err)
			return err
		}
		var customer models.Customer
		if err := tx.Model(&models.Customer{}).Where("id = ?", order.CustomerId).Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		order.CarrierId = carrierId
		if err := tx.Model(&models.Order{}).Where("id = ?", order.Id).Updates(&order).Error; err != nil {
			log.Printf("Update order error: %v\n", err)
			return err
		}
		var orderLines []models.OrderLine
		if err := tx.Model(&models.OrderLine{}).Where("order_id = ?", order.Id).Find(&orderLines).Error; err != nil {
			log.Printf("Find order lines error: %v\n", err)
			return err
		}
		totalAmount := 0.0
		for _, orderLine := range orderLines {
			orderLine.DeliveryTime = time.Now()
			totalAmount += orderLine.Price
			if err := tx.Model(&models.OrderLine{}).Where("id = ?", orderLine.Id).Updates(&orderLine).Error; err != nil {
				log.Printf("Update order line error: %v\n", err)
				return err
			}
		}
		customer.Balance += totalAmount
		customer.DeliveriesNumber++
		if err := tx.Model(&models.Customer{}).Where("id = ?", customer.Id).Updates(&customer).Error; err != nil {
			log.Printf("Update customer error: %v\n", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Delivery transaction error: %v\n", err)
	}
}