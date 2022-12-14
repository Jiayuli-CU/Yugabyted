package postgre

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"fmt"
	"log"

	"gorm.io/gorm"
)

func StockLevel(warehouseId, districtId uint64, threshold, orderNumber int) error {
	db := postgre.GetDB(false)
	var err error
	count := 0

	district := &models.District{
		WarehouseId: warehouseId,
		Id:          districtId,
	}
	if err = db.First(district).Error; err != nil {
		return err
	}

	err = db.Transaction(func(tx *gorm.DB) error {

		nextOrderId := district.NextAvailableOrderNumber
		startOrderId := int(nextOrderId) - orderNumber

		var orderLines []models.OrderLine
		if err = tx.
			Where("warehouse_id = ? AND district_id = ? AND order_id >= ?", warehouseId, districtId, startOrderId).
			Find(&orderLines).Error; err != nil {
			return err
		}

		for _, orderLine := range orderLines {
			itemId := orderLine.ItemId
			stock := &models.Stock{
				WarehouseId: warehouseId,
				ItemId:      itemId,
			}
			if err = tx.First(stock).Error; err != nil {
				return err
			}
			if stock.Quantity < threshold {
				count += 1
			}
		}

		fmt.Printf(" The total number of items in S where its stock quantity at W ID is below the threshold: %d\n", count)
		return nil
	})

	if err != nil {
		log.Printf("Stock level transaction error: %v", err)
	}
	return err
}

func StockLevel2(warehouseId, districtId uint64, threshold, orderNumber int) error {
	db := postgre.GetDB(false)
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error

		district := &models.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		if err = tx.First(district).Error; err != nil {
			return err
		}
		nextOrderId := district.NextAvailableOrderNumber
		startOrderId := int(nextOrderId) - orderNumber

		var orderLines []models.OrderLine
		if err = tx.
			Where("warehouse_id = ? AND district_id = ? AND order_id >= ?", warehouseId, districtId, startOrderId).
			Find(&orderLines).Error; err != nil {
			return err
		}

		count := 0
		for _, orderLine := range orderLines {
			itemId := orderLine.ItemId
			stock := &models.Stock{
				WarehouseId: warehouseId,
				ItemId:      itemId,
			}
			if err = tx.First(stock).Error; err != nil {
				return err
			}
			if stock.Quantity < threshold {
				count += 1
			}
		}

		fmt.Printf(" The total number of items in S where its stock quantity at W ID is below the threshold: %d\n", count)
		return nil
	})

	if err != nil {
		log.Printf("Stock level transaction error: %v", err)
	}
	return err
}
