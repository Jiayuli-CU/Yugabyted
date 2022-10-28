package cassandra

import (
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

func StockLevelTransaction(warehouseId, districtId, stockThreshold, numOrders int) error {
	// find next available order number for (warehouseId, DistrictId)
	var nextOrderNumber int
	var numItemsBelowThreshold int

	GetNextOrderNumberQuery := fmt.Sprintf(`SELECT next_order_number FROM districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)
	if err := session.Query(GetNextOrderNumberQuery).
		Consistency(gocql.Quorum).
		Scan(&nextOrderNumber); err != nil {
		log.Printf("Find next order number error when querying district table: %v\n", err)
		return err
	}

	// collect the set of itemIds
	itemIds := map[int]bool{}
	var orderLines []cassandra.OrderLine

	for orderNumber := nextOrderNumber - numOrders; orderNumber < nextOrderNumber; orderNumber++ {
		// get the set of orderLines of this order
		GetOrderLinesQuery := fmt.Sprintf(`SELECT order_lines FROM orders WHERE warehouse_id = %v AND district_id = %v AND order_id = %v LIMIT 1`, warehouseId, districtId, orderNumber)
		if err := session.Query(GetOrderLinesQuery).
			Consistency(gocql.Quorum).
			Scan(&orderLines); err != nil {
			log.Printf("Find orderlines error when querying orders table: %v\n", err)
			return err
		}

		for _, orderLine := range orderLines {
			itemIds[orderLine.ItemId] = true
		}
	}

	// check storage
	for itemId, _ := range itemIds {
		// get the stock number of this item
		var stockQuantity int
		GetItemStockQuantityQuery := fmt.Sprintf(`SELECT quantity FROM stock_counter WHERE warehouse_id = %v AND item_id = %v LIMIT 1`, warehouseId, itemId)
		if err := session.Query(GetItemStockQuantityQuery).
			Consistency(gocql.Quorum).
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
