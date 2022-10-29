package cassandra

import (
	"cs5424project/store/cassandra"
	"fmt"
	"log"
	"sort"
)

type CustomerBalanceInfo struct {
	Balance     int
	WarehouseId int
	DistrictId  int
	CustomerId  int
}

func TopBalanceTransaction() error {
	/*
		This transaction finds the top-10 customers ranked in descending order of their outstanding balance payments
	*/

	var customerBalanceInfos []CustomerBalanceInfo

	GetAllBalance := `SELECT warehouse_id, district_id, customer_id, balance FROM cs5424_groupI.customer_counters;`

	scanner := session.Query(GetAllBalance).Iter().Scanner()
	for scanner.Next() {
		var (
			_warehouseId int
			_districtId  int
			_customerId  int
			_balance     int
		)

		err := scanner.Scan(&_warehouseId, &_districtId, &_customerId, &_balance)
		if err != nil {
			log.Fatal(err)
		}

		orderInfo := CustomerBalanceInfo{
			WarehouseId: _warehouseId,
			DistrictId:  _districtId,
			CustomerId:  _customerId,
			Balance:     _balance,
		}

		customerBalanceInfos = append(customerBalanceInfos, orderInfo)
	}

	sort.Slice(customerBalanceInfos[:], func(i, j int) bool {
		return customerBalanceInfos[i].Balance > customerBalanceInfos[j].Balance
	})

	for i := 0; i < 10; i++ {
		customerBalanceInfo := customerBalanceInfos[i]
		var (
			customerBasicInfo  cassandra.CustomerInfo
			warehouseBasicInfo cassandra.WarehouseBasicInfo
			districtInfo       cassandra.DistrictInfo
		)

		GetCustomerInfoQuery := fmt.Sprintf(`SELECT basic_info FROM cs5424_groupI.customers WHERE warehouse_id = %v AND district_id = %v AND customer_id = %v LIMIT 1`, customerBalanceInfo.WarehouseId, customerBalanceInfo.DistrictId, customerBalanceInfo.CustomerId)
		if err := session.Query(GetCustomerInfoQuery).
			Scan(&customerBasicInfo); err != nil {
			log.Print(err)
			return err
		}

		GetWDQuery := fmt.Sprintf(`SELECT warehouse_address, district_address FROM cs5424_groupI.districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, customerBalanceInfo.WarehouseId, customerBalanceInfo.DistrictId)
		if err := session.Query(GetWDQuery).
			Scan(&warehouseBasicInfo, &districtInfo); err != nil {
			log.Print(err)
			return err
		}

		fmt.Printf(`
		rank: %v,
		customer name: (%v, %v, %v),
		balance: %v,
		warehouse name: %v,
		district name: %v
		`, i+1, customerBasicInfo.FirstName, customerBasicInfo.MiddleName, customerBasicInfo.LastName,
			customerBalanceInfo.Balance, warehouseBasicInfo.Name, districtInfo.Name)
	}

	return nil
}
