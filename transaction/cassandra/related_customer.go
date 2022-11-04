package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
)

func RelatedCustomerTransaction(ctx context.Context, warehouseId, districtId, customerId int) error {
	var itemIdSets []map[int]bool
	var orderInfosByCustomer []OrderInfo
	// find orders of this customer
	GetOrdersByCustomerQuery := fmt.Sprintf(`SELECT warehouse_id, district_id, order_id, customer_id, order_lines FROM cs5424_groupI.orders 
                                                        WHERE warehouse_id = %v AND district_id = %v AND customer_id = %v`, warehouseId, districtId, customerId)

	scanner := session.Query(GetOrdersByCustomerQuery).WithContext(ctx).Iter().Scanner()
	for scanner.Next() {
		var (
			_warehouseId int
			_districtId  int
			_orderId     int
			_customerId  int
			_orderLines  []cassandra.OrderLine
		)

		err := scanner.Scan(&_warehouseId, &_districtId, &_orderId, &_customerId, &_orderLines)
		if err != nil {
			log.Fatal(err)
		}

		orderInfo := OrderInfo{
			WarehouseId: _warehouseId,
			DistrictId:  _districtId,
			OrderId:     _orderId,
			CustomerId:  _customerId,
			OrderLines:  _orderLines,
		}

		orderInfosByCustomer = append(orderInfosByCustomer, orderInfo)
	}

	// collect set of items
	for _, order := range orderInfosByCustomer {
		itemIdSet := map[int]bool{}
		for _, orderLine := range order.OrderLines {
			itemIdSet[orderLine.ItemId] = true
		}
		itemIdSets = append(itemIdSets, itemIdSet)
	}

	// collect items bought by this customer
	var itemIdsByCustomer map[int]bool
	itemIdsByCustomer = make(map[int]bool)
	for _, itemIdSet := range itemIdSets {
		for itemId, _ := range itemIdSet {
			itemIdsByCustomer[itemId] = true
		}
	}

	var itemIdListByCustomer []int
	for itemId, _ := range itemIdsByCustomer {
		itemIdListByCustomer = append(itemIdListByCustomer, itemId)
	}

	// collect the itemOrders for each item
	var itemOrdersMap map[int][]cassandra.OrderCustomerPK
	scanner = session.Query("SELECT item_id, item_orders FROM cs5424_groupI.item_orders WHERE item_id IN ?", itemIdListByCustomer).
		WithContext(ctx).Iter().Scanner()
	for scanner.Next() {
		var (
			itemId     int
			itemOrders []cassandra.OrderCustomerPK
		)
		scanner.Scan(&itemId, &itemOrders)
		itemOrdersMap[itemId] = itemOrders
	}

	// iterate over each itemIdSet and collect related customers
	relatedCustomers := map[CustomerIdentifier]bool{}
	for _, itemIdSet := range itemIdSets {
		orderSet := map[cassandra.OrderCustomerPK]bool{}

		for itemId, _ := range itemIdSet {
			var itemOrders []cassandra.OrderCustomerPK
			itemOrders = itemOrdersMap[itemId]

			for _, itemOrder := range itemOrders {
				if orderSet[itemOrder] == true {
					// related order
					relatedCustomer := CustomerIdentifier{
						WarehouseId: itemOrder.WarehouseId,
						DistrictId:  itemOrder.DistrictId,
						CustomerId:  itemOrder.CustomerId,
					}

					relatedCustomers[relatedCustomer] = true
				} else {
					orderSet[itemOrder] = true
				}
			}
		}
	}

	var relatedCustomersStr []string
	for relatedCustomer, _ := range relatedCustomers {
		relatedCustomersStr = append(relatedCustomersStr, customerHash(relatedCustomer.WarehouseId, relatedCustomer.DistrictId, relatedCustomer.CustomerId))
	}

	output := RelatedCustomerTransactionOutput{
		TransactionType:            "Related Customer Transaction",
		RelatedCustomerIdentifiers: relatedCustomersStr,
	}

	fmt.Printf("%+v\n", output)
	return nil
}

func customerHash(warehouseId, districtId, customerId int) string {
	return fmt.Sprintf("%d:%d:%d", warehouseId, districtId, customerId)
}
