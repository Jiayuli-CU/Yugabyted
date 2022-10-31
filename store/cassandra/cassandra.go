package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

var session *gocql.Session

const (
	keySpace = "cs5424_groupI"
)

func init() {
	var err error
	cluster := gocql.NewCluster("ap-southeast-1.fbe2e2ee-644d-441a-8bc0-61a134b3f1af.aws.ybdb.io")
	//cluster.Keyspace = keySpace
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "admin",
		Password: "lZdcAJFv1BlkhUMsiz86dLSV-Z1__h",
	}
	//cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("ap-southeast-1")
	cluster.SslOpts = &gocql.SslOptions{
		CaPath:                 "cassandra_root.crt",
		EnableHostVerification: false,
	}
	cluster.Timeout = time.Minute

	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("successfully connected to ycql database")
	}
	//defer session.Close()

	// create keyspaces
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS cs5424_groupI WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	createSchema()
}

//func initTables() {
//	var err error
//
//	//warehouse
//
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.warehouss (id int, name text, street_line_1 text, street_line_2 text, city text, state text, zip text, tax_rate float, year_to_date_amount float, PRIMARY KEY (id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//district
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.districts (id int, warehouse_id int, name text, street_line_1 text, street_line_2 text, city text, state text, zip text, tax_rate float, year_to_date_amount float,next_available_order_number int, PRIMARY KEY (id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//customer
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.customers (id int, warehouse_id int, district_id int, payments_number int, deliveries_number int, first_name text, middle_name text, last_name text, street_line_1 text, street_line_2 text, city text, state text, zip text, phone text, credit_status text, miscellaneous_data text,credit_limit float, discount_rate float, balance float, year_to_date_payment float, create_time time, PRIMARY KEY (warehouse_id, district_id, id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//order
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.orders (id int, warehouse_id int, district_id int, customer_id int, carrier_id int, itemsNumber int, status int, entry_time time, PRIMARY KEY (warehouse_id, district_id, customer_id, id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//item
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.items (id int, image_id int, name text, data text, price float, PRIMARY KEY (id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//order-line
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.order_lines (id int, warehouse_id int, district_id int, order_id int, item_id int, supply_number int, quantity int, delivery_time time, price float, miscellaneous_data text, PRIMARY KEY (warehouse_id, district_id, order_id, id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//
//	//stock
//	err = session.Query("CREATE TABLE IF NOT EXISTS yugabyte.stocks (warehouse_id int, item_id int, year_to_date_quantity_ordered int, quantity int, orders_number int, remote_orders_number  int, district_1_info text, district_2_info text, district_3_info text,district_4_info text, district_5_info text, district_6_info text, district_7_info text, district_8_info text, district_9_info text, district_10_info text, miscellaneous_data text, PRIMARY KEY (warehouse_id, item_id));").Exec()
//	if err != nil {
//		log.Println(err)
//		return
//	}
//}

func GetSession() *gocql.Session {
	return session
}

func CloseSession() {
	session.Close()
}
