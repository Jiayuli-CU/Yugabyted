package transaction

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"gorm.io/gorm"
)

func PopularItem(warehouseId, districtId uint64, orderNumber int) error {
	db := postgre.GetDB()

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
	})
}
