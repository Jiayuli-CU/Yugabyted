package transaction

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"fmt"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"time"
)

func NewOrder(warehouseId, districtId, customerId, total uint64, itemNumbers, supplierWarehouses []uint64, quantities []int) error {
	db := postgre.GetDB()
	local := true
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = false
			break
		}
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		// 在事务中执行一些 db 操作（从这里开始，您应该使用 'tx' 而不是 'db'）
		district := &models.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		err = tx.Last(district).Error
		if err != nil {
			return err
		}
		orderId := district.NextAvailableOrderNumber
		err = tx.Model(district).Update("next_available_order_number", orderId+1).Error
		if err != nil {
			return err
		}

		newOrder := &models.Order{
			Id:          orderId,
			DistrictId:  districtId,
			WarehouseId: warehouseId,
			CustomerId:  customerId,
			EntryTime:   time.Now(),
			ItemsNumber: total,
			Status:      local,
		}

		err = tx.Create(newOrder).Error
		if err != nil {
			return err
		}

		var totalAmount float64
		for idx, itemNumber := range itemNumbers {
			wId := supplierWarehouses[idx]
			stock := &models.Stock{
				WarehouseId: wId,
				Id:          itemNumber,
			}
			err = tx.First(stock).Error
			if err != nil {
				return err
			}

			stockQuantity := stock.Quantity
			adjustedQuantity := stockQuantity - quantities[idx]
			if adjustedQuantity < 10 {
				adjustedQuantity += 100
			}
			stock.Quantity = adjustedQuantity
			stock.OrdersNumber += 1
			if wId != warehouseId {
				stock.RemoteOrdersNumber += 1
			}
			stock.YearToDateQuantityOrdered += quantities[idx]
			// 此处更新有无更好办法？
			err = tx.Model(stock).Updates(stock).Error
			if err != nil {
				return err
			}

			item := &models.Item{
				Id: itemNumber,
			}
			err = tx.First(item).Error
			if err != nil {
				return err
			}
			itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
			totalAmount += itemAmount

			orderLine := &models.OrderLine{
				OrderId:           orderId,
				DistrictId:        districtId,
				WarehouseId:       warehouseId,
				Id:                uint64(idx + 1),
				ItemId:            itemNumber,
				SupplyNumber:      wId,
				Quantity:          quantities[idx],
				Price:             itemAmount,
				MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
			}

			err = tx.Create(orderLine).Error
			if err != nil {
				return err
			}
		}

		localWarehouse := &models.Warehouse{
			Id: warehouseId,
		}
		err = tx.First(localWarehouse).Error
		if err != nil {
			return err
		}
		customer := &models.Customer{
			Id: customerId,
		}
		err = tx.First(customer).Error
		if err != nil {
			return err
		}
		warehouseTax := localWarehouse.TaxRate
		districtTax := district.TaxRate
		discount := customer.DiscountRate
		totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - discount)
		return nil
	})

	return err
}
