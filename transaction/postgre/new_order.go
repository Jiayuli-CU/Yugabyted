package postgre

import (
	"cs5424project/store/postgre"
	"fmt"
	"gorm.io/gorm"
	"time"
)

var db = postgre.GetDB()

func NewOrder(warehouseId, districtId, customerId, total uint64, itemNumbers, supplierWarehouses []uint64, quantities []int) error {

	var warehouseTax, districtTax, discount, totalAmount float64
	var warehouse *postgre.Warehouse
	var customer *postgre.Customer
	var district *postgre.District
	var items []postgre.Item
	var itemInfos []string
	var orderId uint64
	var err error

	local := 1
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = 0
			break
		}
	}

	// promise that get orderId and update orderId is atomic
	err = db.Transaction(func(tx *gorm.DB) error {
		district = &postgre.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		err = tx.First(district).Error
		if err != nil {
			return err
		}
		orderId = district.NextAvailableOrderNumber
		districtTax = district.TaxRate
		if err = tx.Model(district).Update("next_available_order_number", orderId+1).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	warehouse = &postgre.Warehouse{
		Id: warehouseId,
	}
	if err = db.First(warehouse).Error; err != nil {
		return err
	}
	warehouseTax = warehouse.TaxRate

	customer = &postgre.Customer{
		WarehouseId: warehouseId,
		DistrictId:  districtId,
		Id:          customerId,
	}
	if err = db.First(customer).Error; err != nil {
		return err
	}
	discount = customer.DiscountRate

	//create new order
	newOrder := &postgre.Order{
		Id:          orderId,
		DistrictId:  districtId,
		WarehouseId: warehouseId,
		CustomerId:  customerId,
		EntryTime:   time.Now(),
		ItemsNumber: total,
		Status:      local,
	}
	if err = db.Create(newOrder).Error; err != nil {
		return err
	}

	if err = db.Find(&items, itemNumbers).Error; err != nil {
		return err
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		// 在事务中执行一些 db 操作（从这里开始，您应该使用 'tx' 而不是 'db'）

		// get warehouse, district, customer by primary key from database

		// deal with each item:
		// calculate item amount and update information for warehouse and orderline
		for idx, itemNumber := range itemNumbers {

			wId := supplierWarehouses[idx]
			quantity := quantities[idx]

			stock := &postgre.Stock{
				WarehouseId: wId,
				ItemId:      itemNumber,
			}
			if err = tx.First(stock).Error; err != nil {
				return err
			}

			// update stock
			stockQuantity := stock.Quantity
			adjustedQuantity := stockQuantity - quantity
			if adjustedQuantity < 10 {
				adjustedQuantity += 100
			}
			stock.Quantity = adjustedQuantity
			stock.OrdersNumber += 1
			if wId != warehouseId {
				stock.RemoteOrdersNumber += 1
			}
			stock.YearToDateQuantityOrdered += quantity
			// 此处更新有无更好办法？
			err = tx.Model(stock).Updates(stock).Error
			if err != nil {
				return err
			}

			// calculate item and total amount
			item := items[idx]
			itemAmount := float64(quantity) * item.Price
			//itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
			totalAmount += itemAmount

			orderLine := &postgre.OrderLine{
				OrderId:           orderId,
				DistrictId:        districtId,
				WarehouseId:       warehouseId,
				Id:                uint64(idx + 1),
				ItemId:            itemNumber,
				SupplyNumber:      wId,
				Quantity:          quantity,
				Price:             itemAmount,
				MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
			}
			if err = tx.Create(orderLine).Error; err != nil {
				return err
			}
			itemInfo := fmt.Sprintf("item number: %v, item name: %v\n, supplier warehouse: %v, quantity: %v, order line amount: %v, stock quantity: %v\n",
				itemNumber, item.Name, wId, quantity, itemAmount, stockQuantity)
			itemInfos = append(itemInfos, itemInfo)
		}

		totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - discount)
		return nil
	})

	fmt.Println("Customer information:")
	fmt.Printf("	customer identifier: (%d, %d, %d)\n", customer.WarehouseId, customer.DistrictId, customer.Id)
	fmt.Printf("order number: %v, entry date: %v\n", orderId, newOrder.EntryTime)
	fmt.Printf("item numbers: %v, order's total amount: %v\n", itemNumbers, totalAmount)
	for _, info := range itemInfos {
		fmt.Println(info)
	}
	return err
}

func NewOrderV2(warehouseId, districtId, customerId, total uint64, itemNumbers, supplierWarehouses []uint64, quantities []int) error {

	var warehouseTax, districtTax, discount, totalAmount float64
	var warehouse *postgre.Warehouse
	var customer *postgre.Customer
	var district *postgre.District

	local := 1
	for _, w := range supplierWarehouses {
		if warehouseId != w {
			local = 0
			break
		}
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		// 在事务中执行一些 db 操作（从这里开始，您应该使用 'tx' 而不是 'db'）

		// get warehouse, district, customer by primary key from database
		warehouse = &postgre.Warehouse{
			Id: warehouseId,
		}
		if err = tx.First(warehouse).Error; err != nil {
			return err
		}
		warehouseTax = warehouse.TaxRate

		district = &postgre.District{
			WarehouseId: warehouseId,
			Id:          districtId,
		}
		err = tx.First(district).Error
		if err != nil {
			return err
		}
		orderId := district.NextAvailableOrderNumber
		districtTax = district.TaxRate
		if err = tx.Model(district).Update("next_available_order_number", orderId+1).Error; err != nil {
			return err
		}

		customer = &postgre.Customer{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			Id:          customerId,
		}
		if err = tx.First(customer).Error; err != nil {
			return err
		}
		discount = customer.DiscountRate

		//create new order
		newOrder := &postgre.Order{
			Id:          orderId,
			DistrictId:  districtId,
			WarehouseId: warehouseId,
			CustomerId:  customerId,
			EntryTime:   time.Now(),
			ItemsNumber: total,
			Status:      local,
		}
		if err = tx.Create(newOrder).Error; err != nil {
			return err
		}

		// deal with each item:
		// calculate item amount and update information for warehouse and orderline
		for idx, itemNumber := range itemNumbers {

			wId := supplierWarehouses[idx]
			quantity := quantities[idx]

			stock := &postgre.Stock{
				WarehouseId: wId,
				ItemId:      itemNumber,
			}
			if err = tx.First(stock).Error; err != nil {
				return err
			}

			// update stock
			stockQuantity := stock.Quantity
			adjustedQuantity := stockQuantity - quantity
			if adjustedQuantity < 10 {
				adjustedQuantity += 100
			}
			stock.Quantity = adjustedQuantity
			stock.OrdersNumber += 1
			if wId != warehouseId {
				stock.RemoteOrdersNumber += 1
			}
			stock.YearToDateQuantityOrdered += quantity
			// 此处更新有无更好办法？
			err = tx.Model(stock).Updates(stock).Error
			if err != nil {
				return err
			}

			// calculate item and total amount
			item := &postgre.Item{
				Id: itemNumber,
			}
			if err = tx.First(item).Error; err != nil {
				return err
			}
			itemAmount := float64(quantity) * item.Price
			//itemAmount, _ := decimal.NewFromInt(int64(quantities[idx])).Mul(decimal.NewFromFloat(item.Price)).Float64()
			totalAmount += itemAmount

			orderLine := &postgre.OrderLine{
				OrderId:           orderId,
				DistrictId:        districtId,
				WarehouseId:       warehouseId,
				Id:                uint64(idx + 1),
				ItemId:            itemNumber,
				SupplyNumber:      wId,
				Quantity:          quantity,
				Price:             itemAmount,
				MiscellaneousData: fmt.Sprintf("S_DIST_%d", districtId),
			}
			if err = tx.Create(orderLine).Error; err != nil {
				return err
			}
		}

		totalAmount = totalAmount * (1 + warehouseTax + districtTax) * (1 - discount)
		return nil
	})

	fmt.Println("Customer information:")
	fmt.Printf("	customer identifier: W_ID: %d, D_ID: %d, C_ID: %d\n", customer.WarehouseId, customer.DistrictId, customer.Id)

	return err
}
