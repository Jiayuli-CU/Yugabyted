package cassandra

import (
	"cs5424project/store/cassandra"
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

type OrderInfo struct {
	WarehouseId int
	DistrictId  int
	OrderId     int
	CustomerId  int
	OrderLines  []cassandra.OrderLine
}

type CustomerIdentifier struct {
	WarehouseId int
	DistrictId  int
	CustomerId  int
}

func RelatedCustomerTransaction(warehouseId, districtId, customerId int) error {
	var itemIdSets []map[int]bool
	var ordersBycustomer []OrderInfo
	// find orders of this customer
	GetOrdersByCustomerQuery := fmt.Sprintf(`SELECT warehouse_id, district_id, order_id, customer_id FROM orders 
                                                        WHERE warehouse_id = %v AND district_id = %v customer_id = %v`, warehouseId, districtId, customerId)

	if err := session.Query(GetOrdersByCustomerQuery).
		Consistency(gocql.Quorum).
		Scan(&ordersBycustomer); err != nil {
		log.Printf("Find orders by customer: %v\n", err)
		return err
	}

	// collect set of items
	for _, order := range ordersBycustomer {
		itemIdSet := map[int]bool{}
		for _, orderLine := range order.OrderLines {
			itemIdSet[orderLine.ItemId] = true
		}
		itemIdSets = append(itemIdSets, itemIdSet)
	}

	// iterate over all orders
	var allOrderInfos []OrderInfo
	GetAllOderInfosQuery := `SELECT warehouse_id, district_id, order_id, customer_id FROM orders`
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
