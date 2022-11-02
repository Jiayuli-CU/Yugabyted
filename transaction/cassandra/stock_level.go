package cassandra

import (
	"context"
	"cs5424project/store/models"
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func StockLevelTransaction(warehouseId, districtId uint64, threshold, orderNumber int) error {
	var district models.District
	var orderLines []models.OrderLine
	var stock models.Stock

	count := 0
	ctx := context.Background()

	if err := session.Query(fmt.Sprintf(`SELECT * FROM districts WHERE id = %v AND warehouse_id = %v `, districtId, warehouseId)).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(&district); err != nil {
		log.Printf("Find distric error: %v\n", err)
		return err
	}

	nextOrderId := district.NextAvailableOrderNumber
	startOrderId := int(nextOrderId) - orderNumber

	if err := session.Query(fmt.Sprintf(`SELECT * FROM orderlines WHERE id = %v AND warehouse_id = %v AND order_id >= %v`, districtId, warehouseId, startOrderId)).
		WithContext(ctx).Consistency(gocql.Quorum).Scan(&orderLines); err != nil {
		log.Printf("Find orderlines error: %v\n", err)
		return err
	}

	for _, orderline := range orderLines {
		itemId := orderline.ItemId
		if err := session.Query(`SELECT * FROM stocks WHERE WarehouseId = ? AND ItemId = ? LIMIT 1`, warehouseId, itemId).WithContext(ctx).Consistency(gocql.Quorum).Scan(&stock); err != nil {
			log.Printf("Find stock error: %v\n", err)
			return err
		}
		if stock.Quantity < threshold {
			count += 1
		}
	}
	fmt.Printf(" The total number of items in S where its stock quantity at W ID is below the threshold: %d\n", count)

	return nil
}
