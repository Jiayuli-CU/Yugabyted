package postgre

import (
	"cs5424project/store/models"
	"gorm.io/gorm"
	"log"
)

func OrderStatusTransaction(warehouseId, districtId, customerId uint64) error {
	var customer models.Customer
	var order models.Order
	var orderLines []models.OrderLine
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Customer{}).
			Where("id = ? AND warehouse_id = ? AND district_id = ?", customerId, warehouseId, districtId).
			Find(&customer).Error; err != nil {
			log.Printf("Find customer error: %v\n", err)
			return err
		}
		if err := tx.Model(&models.Order{}).Where("customer_id = ? AND warehouse_id = ? AND district_id = ?", customer.Id, customer.WarehouseId, customer.DistrictId).Last(&order).Error; err != nil {
			log.Printf("Last order error: %v\n", err)
			return err
		}
		if err := tx.Model(&models.OrderLine{}).Where("order_id = ? AND warehouse_id = ? AND district_id = ?", order.Id, order.WarehouseId, order.DistrictId).Find(&orderLines).Error; err != nil {
			log.Printf("Find order lines error: %v\n", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Order status transaction error: %v\n", err)
		return nil
	}
	log.Printf("Customer info: first name = %v, middle name = %v, last name = %v, balance = %v\n",
		customer.FirstName,
		customer.MiddleName,
		customer.LastName,
		customer.Balance,
	)
	log.Printf("Customer last order info: order id = %v, entry time = %v, carrier id = %v",
		order.Id,
		order.EntryTime,
		order.CarrierId,
	)
	for _, orderLine := range orderLines {
		log.Printf("Customer order item info: item id = %v, warehouse id = %v, quantity ordered = %v, total price = %v, delivery time = %v\n",
			orderLine.ItemId,
			orderLine.WarehouseId,
			orderLine.Quantity,
			orderLine.Price,
			orderLine.DeliveryTime,
		)
	}
	return err
}
