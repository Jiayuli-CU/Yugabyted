package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
)

func StockLevelTransaction(ctx context.Context, warehouseId, districtId, stockThreshold, numOrders int) error {
	// find next available order number for (warehouseId, DistrictId)
	var nextOrderNumber int
	var numItemsBelowThreshold int

	GetNextOrderNumberQuery := fmt.Sprintf(`SELECT next_order_number FROM cs5424_groupI.districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)
	if err := session.Query(GetNextOrderNumberQuery).
		WithContext(ctx).
		Scan(&nextOrderNumber); err != nil {
		log.Printf("Find next order number error when querying district table: %v\n", err)
		return err
	}

	// collect the set of itemIds
	itemIds := map[int]bool{}
	var orderLinesList [][]cassandra.OrderLine
	GetOrderLinesListQuery := fmt.Sprintf(`SELECT order_lines FROM cs5424_groupI.orders 
                   WHERE warehouse_id = %v AND district_id = %v AND order_id > %v AND order_id < %v`,
		warehouseId, districtId, nextOrderNumber-numOrders-1, nextOrderNumber)
	if err := session.Query(GetOrderLinesListQuery).
		Scan(&orderLinesList); err != nil {
		log.Printf("Find orderlines error when querying orders table: %v\n", err)
		return err
	}

	for _, orderLines := range orderLinesList {
		for _, orderLine := range orderLines {
			itemIds[orderLine.ItemId] = true
		}
	}

	// check storage
	for itemId, _ := range itemIds {
		// get the stock number of this item
		var stockQuantity int
		GetItemStockQuantityQuery := fmt.Sprintf(`SELECT quantity FROM cs5424_groupI.stock_counters WHERE warehouse_id = %v AND item_id = %v LIMIT 1`, warehouseId, itemId)
		if err := session.Query(GetItemStockQuantityQuery).
			WithContext(ctx).
			Scan(&stockQuantity); err != nil {
			log.Printf("Find item quantity error when querying stock_counter table: %v\n", err)
			return err
		}

		if stockQuantity < stockThreshold {
			numItemsBelowThreshold++
		}
	}

	fmt.Printf("Number of items below threshold %v for warehouseId: %v : %v\n", stockThreshold, warehouseId, numItemsBelowThreshold)
	return nil
}
