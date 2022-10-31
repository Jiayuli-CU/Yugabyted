package postgre

import (
	"cs5424project/store/models"
	"fmt"
	"log"

	"gorm.io/gorm"
)

func PopularItem(warehouseId, districtId uint64, orderNumber int) error {
	var orders []models.Order
	var quantities []int
	var orderLines []models.OrderLine
	var orderInfos []string

	district := &models.District{
		WarehouseId: warehouseId,
		Id:          districtId,
	}
	if err := db.First(district).Error; err != nil {
		return err

	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		nextOrderId := district.NextAvailableOrderNumber
		startOrderId := int(nextOrderId) - orderNumber

		if err = tx.
			Where("warehouse_id = ? AND district_id = ? AND id >= ?", warehouseId, districtId, startOrderId).
			Find(&orders).Error; err != nil {
			return err
		}

		for i := startOrderId; i < int(nextOrderId); i++ {
			orderLine := &models.OrderLine{}
			if err = tx.Where("warehouse_id = ? AND district_id = ? AND order_id = ?", warehouseId, districtId, i).
				Order("quantity desc").Limit(1).Find(orderLine).Error; err != nil {
				return err
			}
			quantity := orderLine.Quantity
			orderLines = append(orderLines, *orderLine)
			quantities = append(quantities, quantity)
		}
		return nil
	})

	if err != nil {
		log.Printf("Popular item transaction error: %v", err)
		return err
	}

	for i, order := range orders {
		orderId := order.Id
		currOrderLine := orderLines[i]
		itemId := currOrderLine.ItemId
		item := &models.Item{
			Id: itemId,
		}
		if err = db.First(item).Error; err != nil {
			return err
		}
		itemName := item.Name

		entryTime := order.EntryTime
		customerId := order.CustomerId
		customer := &models.Customer{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			Id:          customerId,
		}
		if err = db.First(customer).Error; err != nil {
			return err
		}
		customerFirst, customerMiddle, customerLast := customer.FirstName, customer.MiddleName, customer.LastName
		orderinfo := fmt.Sprintf("Order number: %v, Order entryTime: %v\nCustomerFirst: %v, CustomerMiddle: %v, CustomerLast: %v\nPopularItem: %v,Quantity: %v", orderId, entryTime, customerFirst, customerMiddle, customerLast, itemName, quantities[i])
		orderInfos = append(orderInfos, orderinfo)
	}
	fmt.Println("District information:")
	fmt.Printf("District identifier: (%d, %d)\n", warehouseId, districtId)
	fmt.Printf("Number of last orders to be examined: %d\n", orderNumber)
	for _, info := range orderInfos {
		fmt.Println(info)
	}

	return err
}

func PopularItem2(warehouseId, districtId uint64, orderNumber int) error {

	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		district := &models.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		if err = tx.First(district).Error; err != nil {
			return err
		}
		nextOrderId := district.Id
		startOrderId := int(nextOrderId) - orderNumber

		var orders []models.Order
		if err = tx.
			Where("warehouse_id = ? AND district_id = ? AND id >= ?", warehouseId, districtId, startOrderId).
			Find(&orders).Error; err != nil {
			return err
		}

		for i := startOrderId; i < orderNumber; i++ {
			orderLine := &models.OrderLine{}
			// ???
			if err = tx.Where("warehouse_id = ? AND district_id = ? AND order_id = ?", warehouseId, districtId, i).
				Order("quantity desc").Limit(1).Find(orderLine).Error; err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Printf("Popular item transaction error: %v", err)
	}
	return err
}
