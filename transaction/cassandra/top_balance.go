package cassandra

import (
	"context"
	"cs5424project/store/cassandra"
	"fmt"
	"log"
	"sort"
	"sync"
)

type CustomerBalanceInfo struct {
	Balance     int
	WarehouseId int
	DistrictId  int
	CustomerId  int
}

func TopBalanceTransaction(ctx context.Context) error {
	/*
		This transaction finds the top-10 customers ranked in descending order of their outstanding balance payments
	*/

	wg := &sync.WaitGroup{}
	top100CustomerBalanceInfos := make([]CustomerBalanceInfo, 100)

	for warehouseId := 1; warehouseId <= 10; warehouseId++ {
		wg.Add(1)

		go func(warehouseId int) {
			defer wg.Done()

			top10PerWarehouse := getTopTenBalancePerWarehouse(ctx, warehouseId)

			for idx, customerBalanceInfo := range top10PerWarehouse {
				idxIn100 := (warehouseId-1)*10 + idx
				top100CustomerBalanceInfos[idxIn100] = customerBalanceInfo
			}

		}(warehouseId)
	}

	wg.Wait()

	sort.Slice(top100CustomerBalanceInfos, func(i, j int) bool {
		return top100CustomerBalanceInfos[i].Balance > top100CustomerBalanceInfos[j].Balance
	})

	var outputs []TopBalanceTransactionOutput

	for i := 0; i < 10; i++ {
		customerBalanceInfo := top100CustomerBalanceInfos[i]
		var (
			customerBasicInfo  cassandra.CustomerInfo
			warehouseBasicInfo cassandra.WarehouseBasicInfo
			districtInfo       cassandra.DistrictInfo
		)

		GetCustomerInfoQuery := fmt.Sprintf(`SELECT basic_info FROM cs5424_groupl.customers WHERE warehouse_id = %v AND district_id = %v AND customer_id = %v LIMIT 1`, customerBalanceInfo.WarehouseId, customerBalanceInfo.DistrictId, customerBalanceInfo.CustomerId)
		if err := session.Query(GetCustomerInfoQuery).
			Scan(&customerBasicInfo); err != nil {
			log.Print(err)
			return err
		}

		GetWDQuery := fmt.Sprintf(`SELECT warehouse_address, district_address FROM cs5424_groupl.districts WHERE warehouse_id = %v AND district_id = %v LIMIT 1`, customerBalanceInfo.WarehouseId, customerBalanceInfo.DistrictId)
		if err := session.Query(GetWDQuery).
			Scan(&warehouseBasicInfo, &districtInfo); err != nil {
			log.Print(err)
			return err
		}

		output := TopBalanceTransactionOutput{
			TransactionType: "Top Balance Transaction",
			FirstName:       customerBasicInfo.FirstName,
			MiddleName:      customerBasicInfo.MiddleName,
			LastName:        customerBasicInfo.LastName,
			Balance:         float32(customerBalanceInfo.Balance) / 100,
			WarehouseName:   warehouseBasicInfo.Name,
			DistrictName:    districtInfo.Name,
		}

		outputs = append(outputs, output)
	}

	fmt.Printf("%+v\n", outputs)
	println()

	return nil
}

func getTopTenBalancePerWarehouse(ctx context.Context, warehouseId int) []CustomerBalanceInfo {
	var customerBalanceInfos []CustomerBalanceInfo
	GetAllBalancePerWarehouse := fmt.Sprintf(`SELECT warehouse_id, district_id, customer_id, balance FROM cs5424_groupl.customer_counters WHERE warehouse_id = %v`, warehouseId)
	scanner := session.Query(GetAllBalancePerWarehouse).WithContext(ctx).Iter().Scanner()
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

	sort.Slice(customerBalanceInfos, func(i, j int) bool {
		return customerBalanceInfos[i].Balance > customerBalanceInfos[j].Balance
	})

	return customerBalanceInfos[:10]
}
