package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
)

func StockLevelTransaction(ctx context.Context, warehouseId, districtId, stockThreshold, numOrders int) error {
	// find next available order number for (warehouseId, DistrictId)
	var numItemsBelowThreshold int

	// collect the set of itemIds
	itemIds := map[int]bool{}
	var orderLinesList [][]cassandra.OrderLine
	GetOrderLinesListQuery := fmt.Sprintf(`SELECT order_lines FROM cs5424_groupI.orders 
                   WHERE warehouse_id = %v AND district_id = %v ORDER BY order_id desc LIMIT %v`,
		warehouseId, districtId, numOrders)
	scanner := session.Query(GetOrderLinesListQuery).WithContext(ctx).Iter().Scanner()
	for scanner.Next() {
		var orderLines []cassandra.OrderLine
		scanner.Scan(&orderLines)
		orderLinesList = append(orderLinesList, orderLines)
	}

	for _, orderLines := range orderLinesList {
		for _, orderLine := range orderLines {
			itemIds[orderLine.ItemId] = true
		}
	}

	// check storage
	var items []int
	for itemId, _ := range itemIds {
		items = append(items, itemId)
	}

	scanner = session.Query(`SELECT quantity FROM cs5424_groupI.stock_counters WHERE warehouse_id = ? AND item_id IN ?`, warehouseId, items).
		WithContext(ctx).Iter().Scanner()
	for scanner.Next() {
		var quantity int
		scanner.Scan(&quantity)
		if quantity < stockThreshold {
			numItemsBelowThreshold++
		}
	}

	fmt.Printf("TransactionType:Stock Level Transaction\t")
	fmt.Printf("Number of items below threshold %v for warehouseId: %v : %v\n", stockThreshold, warehouseId, numItemsBelowThreshold)
	println()
	return nil
}
