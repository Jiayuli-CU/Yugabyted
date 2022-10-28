package cassandra

import (
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

func PopularItemTransaction(warehouseId, districtId, numOrders int) error {
	fmt.Printf("warehouseId: %v, districtId: %v\n", warehouseId, districtId)
	fmt.Printf("Number of last oders to be examined: %v\n", numOrders)

	// find next available order number for (warehouseId, DistrictId)
	var nextOrderNumber int

	GetNextOrderNumberQuery := fmt.Sprintf(`SELECT next_order_number FROM districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)
	if err := session.Query(GetNextOrderNumberQuery).
		Consistency(gocql.Quorum).
		Scan(&nextOrderNumber); err != nil {
		log.Printf("Find next order number error when querying district table: %v\n", err)
		return err
	}

	// collect the set of itemIds
	itemIdSetForEachOrder := map[int]map[int]bool{}
	var orderEntryTime time.Time
	var firstName, middleName, lastName string
	var orderLines []cassandra.OrderLine
	popularItemIds := map[int]bool{}
	itemIdToName := map[int]string{}

	fmt.Println("For each order:")
	// for each order
	for orderNumber := nextOrderNumber - numOrders; orderNumber < nextOrderNumber; orderNumber++ {
		// get the set of orderLines of this order
		GetOrderLinesQuery := fmt.Sprintf(`SELECT order_lines, entry_time, first_name, middle_name, last_name FROM orders WHERE warehouse_id = %v AND district_id = %v AND order_id = %v LIMIT 1`, warehouseId, districtId, orderNumber)
		if err := session.Query(GetOrderLinesQuery).
			Consistency(gocql.Quorum).
			Scan(&orderLines, &orderEntryTime, &firstName, &middleName, &lastName); err != nil {
			log.Printf("Find orderlines error when querying orders table: %v\n", err)
			return err
		}

		fmt.Printf("orderId: %v, entry date and time: %v\n", orderNumber, orderEntryTime)
		fmt.Printf("%v, %v, %v", firstName, middleName, lastName)

		itemIds := map[int]bool{}

		// find the max quantity for this order
		var maxQuantity int

		for _, orderLine := range orderLines {
			itemIds[orderLine.ItemId] = true

			if orderLine.Quantity > maxQuantity {
				maxQuantity = orderLine.Quantity
			}
		}

		// find the popular item for this order (could be more than 1 popular item)
		for _, orderLine := range orderLines {
			if orderLine.Quantity == maxQuantity {
				popularItemIds[orderLine.ItemId] = true
				//popularItemIds.Add(orderLine.ItemId)
				itemIdToName[orderLine.ItemId] = orderLine.ItemName
				fmt.Printf("ItemName: %v,\tQuantity: %v", orderLine.ItemName, orderLine.Quantity)
			}
		}

		itemIdSetForEachOrder[orderNumber] = itemIds
	}

	// calculate the percentage of examined orders that contain each popular item
	for itemId, _ := range popularItemIds {
		itemName := itemIdToName[itemId]
		count := 0

		for _, itemIdSet := range itemIdSetForEachOrder {
			if itemIdSet[itemId] == true {
				count++
			}
		}

		percentage := count * 100.0 / numOrders
		fmt.Printf("popular item name: %v, percentage: %v\n", itemName, percentage)
	}

	return nil
}
