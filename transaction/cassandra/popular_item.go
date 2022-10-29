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

	GetNextOrderNumberQuery := fmt.Sprintf(`SELECT next_order_number FROM cs5424_groupI.districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)
	if err := session.Query(GetNextOrderNumberQuery).
		Consistency(gocql.Quorum).
		Scan(&nextOrderNumber); err != nil {
		log.Printf("Find next order number error when querying district table: %v\n", err)
		return err
	}

	// collect the set of itemIds
	itemIdSetForEachOrder := map[int]map[int]bool{}
	popularItemIds := map[int]bool{}
	itemIdToName := map[int]string{}

	fmt.Println("For each order:")

	// get all required orders
	var orders []orderInfo
	GetOrdersQuery := fmt.Sprintf(`SELECT order_id, order_lines, entry_time, first_name, middle_name, last_name FROM cs5424_groupI.orders 
                                                                             WHERE warehouse_id = %v AND district_id = %v AND order_id > %v AND order_id < %v`,
		warehouseId, districtId, nextOrderNumber-numOrders-1, nextOrderNumber)
	scanner := session.Query(GetOrdersQuery).Iter().Scanner()
	for scanner.Next() {
		var (
			_orderId    int
			_orderLines []cassandra.OrderLine
			_entryTime  time.Time
			_firstName  string
			_middleName string
			_lastName   string
		)

		err := scanner.Scan(&_orderId, &_orderLines, &_entryTime, &_firstName, &_middleName, &_lastName)
		if err != nil {
			log.Fatal(err)
		}

		order := orderInfo{
			orderId:    _orderId,
			orderLines: _orderLines,
			entryTime:  _entryTime,
			firstName:  _firstName,
			middleName: _middleName,
			lastName:   _lastName,
		}

		orders = append(orders, order)
	}

	// for each order
	for _, order := range orders {
		// get the set of orderLines of this order
		fmt.Printf("orderId: %v, entry date and time: %v\n", order.orderId, order.entryTime)
		fmt.Printf("%v, %v, %v", order.firstName, order.middleName, order.lastName)

		itemIds := map[int]bool{}

		// find the max quantity for this order
		var maxQuantity int

		for _, orderLine := range order.orderLines {
			itemIds[orderLine.ItemId] = true

			if orderLine.Quantity > maxQuantity {
				maxQuantity = orderLine.Quantity
			}
		}

		// find the popular item for this order (could be more than 1 popular item)
		for _, orderLine := range order.orderLines {
			if orderLine.Quantity == maxQuantity {
				popularItemIds[orderLine.ItemId] = true
				itemIdToName[orderLine.ItemId] = orderLine.ItemName
				fmt.Printf("ItemName: %v,\tQuantity: %v", orderLine.ItemName, orderLine.Quantity)
			}
		}

		itemIdSetForEachOrder[order.orderId] = itemIds
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

type orderInfo struct {
	orderId    int
	orderLines []cassandra.OrderLine
	entryTime  time.Time
	firstName  string
	middleName string
	lastName   string
}
