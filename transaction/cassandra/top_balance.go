package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"sort"
)

type CustomerBalanceInfo struct {
	Balance       int
	FirstName     string
	MiddleName    string
	LastName      string
	WarehouseName string
	DistrictName  string
}

func TopBalanceTransaction() error {
	/*
		This transaction finds the top-10 customers ranked in descending order of their outstanding balance payments
	*/

	var customerBalanceInfos []CustomerBalanceInfo

	GetTop10BalancePerPartition := `SELECT * FROM customer_balance PER PARTITION LIMIT 10;`

	if err := session.Query(GetTop10BalancePerPartition).
		Consistency(gocql.Quorum).
		Scan(&customerBalanceInfos); err != nil {
		log.Printf("Find top 10 balance for each partation: %v\n", err)
		return err
	}

	sort.Slice(customerBalanceInfos[:], func(i, j int) bool {
		return customerBalanceInfos[i].Balance > customerBalanceInfos[j].Balance
	})

	for i := 0; i < 10; i++ {
		c_info := customerBalanceInfos[i]
		fmt.Printf(`
		rank: %v,
		customer name: (%v, %v, %v),
		balance: %v,
		warehouse name: %v,
		district name: %v
		`, i+1, c_info.FirstName, c_info.MiddleName, c_info.LastName, c_info.Balance, c_info.WarehouseName, c_info.DistrictName)
	}

	return nil
}
