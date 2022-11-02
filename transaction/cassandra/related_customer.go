package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
	"sync"
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

	fmt.Printf("len of orderinfos: %v\n", len(orderInfosByCustomer))
	// collect set of items
	for _, order := range orderInfosByCustomer {
		itemIdSet := map[int]bool{}
		for _, orderLine := range order.OrderLines {
			itemIdSet[orderLine.ItemId] = true
		}
		itemIdSets = append(itemIdSets, itemIdSet)
	}

	for _, itemSet := range itemIdSets {
		fmt.Println(itemSet)
	}

	// iterate over all orders
	relatedCustomerList := make([]map[string]bool, 10)
	var wg sync.WaitGroup
	for wId := 1; wId <= 10; wId++ {
		if wId == warehouseId {
			continue
		}
		relatedCustomerList[wId-1] = make(map[string]bool)
		wg.Add(1)
		go checkRelatedCustomerPerWarehouse(&wg, wId, itemIdSets, relatedCustomerList[wId-1])
	}

	wg.Wait()

	var relatedCustomers []string
	for i, m := range relatedCustomerList {
		if i+1 == warehouseId {
			continue
		}
		for customer, _ := range m {
			relatedCustomers = append(relatedCustomers, customer)
		}
	}

	output := RelatedCustomerTransactionOutput{
		TransactionType:            "Related Customer Transaction",
		RelatedCustomerIdentifiers: relatedCustomers,
	}

	fmt.Printf("%+v\n", output)
	return nil
}

func checkRelatedCustomerPerWarehouse(wg *sync.WaitGroup, warehouseId int, itemIdSets []map[int]bool, relatedCustomers map[string]bool) {
	defer wg.Done()

	fmt.Printf("start: %v\n", warehouseId)
	var allOrderInfos []OrderInfo
	GetAllOderInfosQuery := fmt.Sprintf(`SELECT district_id, order_id, customer_id, order_lines FROM cs5424_groupI.orders WHERE warehouse_id = %v`, warehouseId)
	scanner := session.Query(GetAllOderInfosQuery).Iter().Scanner()
	for scanner.Next() {
		var (
			districtId int
			orderId    int
			customerId int
			orderLines []cassandra.OrderLine
		)

		scanner.Scan(&districtId, &orderId, &customerId, &orderLines)

		orderInfo := OrderInfo{
			WarehouseId: warehouseId,
			DistrictId:  districtId,
			OrderId:     orderId,
			CustomerId:  customerId,
			OrderLines:  orderLines,
		}

		allOrderInfos = append(allOrderInfos, orderInfo)
	}

	for _, orderInfo := range allOrderInfos {

		customerPrimaryKey := fmt.Sprintf("%d:%d:%d", orderInfo.WarehouseId, orderInfo.DistrictId, orderInfo.CustomerId)

		// check if this customer is already a related customer
		if relatedCustomers[customerPrimaryKey] {
			continue
		}

		for _, itemIdSet := range itemIdSets {
			if relatedCustomers[customerPrimaryKey] {
				break
			}
			count := 0
			for _, orderLine := range orderInfo.OrderLines {
				if itemIdSet[orderLine.ItemId] {
					count++
					if count >= 2 {
						relatedCustomers[customerPrimaryKey] = true
						break
					}
				}
			}
		}
	}
}
