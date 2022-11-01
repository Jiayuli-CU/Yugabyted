package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
	"time"
)

func PopularItemTransaction(ctx context.Context, warehouseId, districtId, numOrders int) error {
	// find next available order number for (warehouseId, DistrictId)
	var nextOrderNumber int

	GetNextOrderNumberQuery := fmt.Sprintf(`SELECT next_order_number FROM cs5424_groupI.districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, warehouseId, districtId)
	if err := session.Query(GetNextOrderNumberQuery).
		WithContext(ctx).
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
	var orderInfos []OrderInfoForPopularItemTransaction
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

		var _popularItemsForThisOrder []ItemInfoForPopularItemTransaction
		itemIds := map[int]bool{}

		// find the max quantity for this order
		var maxQuantity int

		for _, orderLine := range _orderLines {
			itemIds[orderLine.ItemId] = true

			if orderLine.Quantity > maxQuantity {
				maxQuantity = orderLine.Quantity
			}
		}

		// find the popular item for this order (could be more than 1 popular item)
		for _, orderLine := range _orderLines {
			if orderLine.Quantity == maxQuantity {
				popularItemIds[orderLine.ItemId] = true
				itemIdToName[orderLine.ItemId] = orderLine.ItemName
				popularItem := ItemInfoForPopularItemTransaction{
					ItemName: orderLine.ItemName,
					Quantity: orderLine.Quantity,
				}
				_popularItemsForThisOrder = append(_popularItemsForThisOrder, popularItem)
			}
		}

		orderInfo := OrderInfoForPopularItemTransaction{
			OrderId:                  _orderId,
			EntryTime:                _entryTime,
			FirstName:                _firstName,
			MiddleName:               _middleName,
			LastName:                 _lastName,
			PopularItemsForThisOrder: _popularItemsForThisOrder,
		}

		itemIdSetForEachOrder[orderInfo.OrderId] = itemIds

		orderInfos = append(orderInfos, orderInfo)
	}

	var popularItemPercentages []PopularItemPercentage
	// calculate the percentage of examined orders that contain each popular item
	for itemId, _ := range popularItemIds {
		itemName := itemIdToName[itemId]
		count := 0

		for _, itemIdSet := range itemIdSetForEachOrder {
			if itemIdSet[itemId] == true {
				count++
			}
		}

		percentage := float32(count) * 100 / float32(numOrders)
		itemPercentage := PopularItemPercentage{
			ItemName:   itemName,
			Percentage: percentage,
		}
		popularItemPercentages = append(popularItemPercentages, itemPercentage)
	}

	output := PopularItemTransactionOutput{
		TransactionType:            "Popular Item Transaction",
		WarehouseId:                warehouseId,
		DistrictId:                 districtId,
		NumberOfOrdersToBeExamined: numOrders,
		OrderInfos:                 orderInfos,
		PopularItemPercentages:     popularItemPercentages,
	}

	fmt.Printf("%+v\n", output)
	println()

	return nil
}
