package cassandra

import (
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

func RelatedCustomerTransaction(warehouseId, districtId, customerId int) error {
	var itemIdSets []map[int]bool
	var orderInfosByCustomer []OrderInfo
	// find orders of this customer
	GetOrdersByCustomerQuery := fmt.Sprintf(`SELECT warehouse_id, district_id, order_id, customer_id, order_lines FROM cs5424_groupI.orders 
                                                        WHERE warehouse_id = %v AND district_id = %v customer_id = %v`, warehouseId, districtId, customerId)

	scanner := session.Query(GetOrdersByCustomerQuery).Iter().Scanner()
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

	// iterate over all orders
	var allOrderInfos []OrderInfo
	GetAllOderInfosQuery := `SELECT warehouse_id, district_id, order_id, customer_id FROM cs5424_groupI.orders`
	if err := session.Query(GetAllOderInfosQuery).
		Consistency(gocql.Quorum).
		Scan(&allOrderInfos); err != nil {
		log.Print(err)
		return err
	}

	var relatedCustomers map[CustomerIdentifier]bool

	for _, orderInfo := range allOrderInfos {
		// check if it is in the same warehouse
		if orderInfo.WarehouseId == warehouseId {
			continue
		}

		customerIdentifier := CustomerIdentifier{
			WarehouseId: orderInfo.WarehouseId,
			DistrictId:  orderInfo.DistrictId,
			CustomerId:  orderInfo.CustomerId,
		}

		// check if this customer is already a related customer
		if relatedCustomers[customerIdentifier] == true {
			continue
		}

		for _, itemIdSet := range itemIdSets {
			if relatedCustomers[customerIdentifier] == true {
				break
			}

			count := 0
			for _, orderLine := range orderInfo.OrderLines {
				if itemIdSet[orderLine.ItemId] == true {
					count++
					if count >= 2 {
						relatedCustomers[customerIdentifier] = true
						break
					}
				}
			}
		}
	}

	for relatedCustomer, _ := range relatedCustomers {
		fmt.Printf("(warehouseId: %v, districtId: %v, customerId: %v)",
			relatedCustomer.WarehouseId, relatedCustomer.DistrictId, relatedCustomer.CustomerId)
	}

	return nil
}
