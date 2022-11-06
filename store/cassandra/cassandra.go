package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"os"
	"time"
)

var session *gocql.Session

func init() {
	getArgsAndCreateSession()
}

func getArgsAndCreateSession() {
	args := os.Args[1:]
	ips := args[:len(args)-2]
	username := args[5]
	password := args[6]
	createSession(ips, username, password)
}

func createSession(ips []string, username, password string) {
	var err error
	cluster := gocql.NewCluster(ips...)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Timeout = time.Minute

	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("successfully connected to ycql database")
	}
	//defer session.Close()

	return
}

func GetSession() *gocql.Session {
	return session
}

func CloseSession() {
	session.Close()
}

func DropTablesIfExists() {
	dropWarehouse := `drop table  IF EXISTS cs5424_groupl.warehouse_counter`
	dropDistrict := `drop table  IF EXISTS cs5424_groupl.districts`
	dropDistrictCounter := `drop table  IF EXISTS cs5424_groupl.district_counter`
	dropCustomers := `drop table  IF EXISTS cs5424_groupl.customers`
	dropItems := `drop table  IF EXISTS cs5424_groupl.items`
	dropOrders := `drop table  IF EXISTS cs5424_groupl.orders`
	dropStocks := `drop table  IF EXISTS cs5424_groupl.stocks`
	dropStockCounters := `drop table  IF EXISTS cs5424_groupl.stock_counters`
	dropCustomerCounters := `drop table  IF EXISTS cs5424_groupl.customer_counters`

	err := session.Query(dropWarehouse)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropDistrict)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropDistrictCounter)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropCustomers)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropCustomerCounters)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropItems)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropOrders)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropStocks)
	if err != nil {
		fmt.Println(err)
	}

	err = session.Query(dropStockCounters)
	if err != nil {
		fmt.Println(err)
	}
}
