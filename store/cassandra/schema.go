package cassandra

import (
	"fmt"
	"log"
)

func CreateSchema() {

	var err error

	err = session.Query("CREATE KEYSPACE IF NOT EXISTS cs5424_groupl WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	warehouseAddressType := "CREATE TYPE IF NOT EXISTS cs5424_groupl.warehouse_info " +
		"(name text, street_1 text, street_2 text, city text, state text, zip text);"
	err = session.Query(warehouseAddressType).Exec()
	if err != nil {
		log.Println(err)
	}

	districtAddressType := "CREATE TYPE IF NOT EXISTS cs5424_groupl.district_info " +
		"(name text, street_1 text, street_2 text, city text, state text, zip text);"
	err = session.Query(districtAddressType).Exec()
	if err != nil {
		log.Println(err)
	}

	warehouseCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.warehouse_counter " +
		"(warehouse_id int, warehouse_year_to_date_payment counter, " +
		"PRIMARY KEY (warehouse_id));"
	err = session.Query(warehouseCounterQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	districtCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.district_counter " +
		"(warehouse_id int, district_id int, district_year_to_date_payment counter, " +
		"PRIMARY KEY ((warehouse_id), district_id));"
	err = session.Query(districtCounterQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	districtQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.districts " +
		"(warehouse_id int, district_id int, next_order_number int, district_address FROZEN<district_info>, district_tax_rate float, warehouse_address FROZEN<warehouse_info> static, warehouse_tax_rate float static, next_delivery_order_id int, " +
		"PRIMARY KEY ((warehouse_id), district_id));"
	err = session.Query(districtQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	customerInfoType := "CREATE TYPE IF NOT EXISTS cs5424_groupl.customer_info " +
		"(first_name text, middle_name text, last_name text, street_1 text, street_2 text, city text, state text, zip text, phone text, since timestamp, credit text, credit_limit float);"
	err = session.Query(customerInfoType).Exec()
	if err != nil {
		log.Println(err)
	}

	customerQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.customers " +
		"(warehouse_id int, district_id int, customer_id int, basic_info FROZEN<cs5424_groupl.customer_info>, discount_rate float, miscellaneous_data text, last_order_id int, " +
		"PRIMARY KEY ((warehouse_id), district_id, customer_id));"
	err = session.Query(customerQuery).Exec()
	if err != nil {
		log.Println(err)
	}
	fmt.Println("customer success created")

	customerCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.customer_counters " +
		"(warehouse_id int, district_id int, customer_id int, payment_count counter, delivery_count counter, balance counter, year_to_date_payment counter, " +
		"PRIMARY KEY ((warehouse_id), district_id, customer_id));"
	err = session.Query(customerCounterQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	orderLineTypeQuery := "CREATE TYPE IF NOT EXISTS cs5424_groupl.order_line " +
		"(order_line_id int, item_id int, item_name text, amount int, supply_warehouse_id int, quantity int, miscellaneous_data text);"

	err = session.Query(orderLineTypeQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	orderQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.orders " +
		"(warehouse_id int, district_id int, order_id int, customer_id int, first_name text, middle_name text, last_name text, carrier_id int, items_number int, all_local int, entry_time timestamp, order_lines set<FROZEN<order_line>>, delivery_time timestamp, total_amount int, " +
		"PRIMARY KEY ((warehouse_id, district_id), order_id));"
	err = session.Query(orderQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	customerIndex := "CREATE INDEX order_customer_index ON cs5424_groupl.orders (customer_id) WITH transactions = {'enabled': 'false', 'consistency_level': 'user_enforced'};"
	err = session.Query(customerIndex).Exec()
	if err != nil {
		log.Println(err)
	}

	stockInfoTypeQuery := "CREATE TYPE IF NOT EXISTS cs5424_groupl.stock_info " +
		"(district_1_info text, district_2_info text, district_3_info text,district_4_info text, district_5_info text, district_6_info text, district_7_info text, district_8_info text, district_9_info text, district_10_info text, miscellaneous_data text);"
	err = session.Query(stockInfoTypeQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	stockQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.stocks " +
		"(warehouse_id int, item_id int, basic_info FROZEN<cs5424_groupl.stock_info>, " +
		"PRIMARY KEY (warehouse_id, item_id));"
	err = session.Query(stockQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	stockCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupl.stock_counters " +
		"(warehouse_id int, item_id int, order_count counter, remote_count counter, quantity counter, total_quantity counter, " +
		"PRIMARY KEY (warehouse_id, item_id));"
	err = session.Query(stockCounterQuery).Exec()
	if err != nil {
		log.Println(err)
	}

	itemOrderType := "CREATE TYPE IF NOT EXISTS cs5424_groupl.item_order " +
		"(warehouse_id int, district_id int, order_id int, customer_id int);"
	err = session.Query(itemOrderType).Exec()
	if err != nil {
		log.Println(err)
	}

	createItemsCmd := "CREATE TABLE IF NOT EXISTS cs5424_groupl.items (" +
		" item_id int, " +
		" item_name text, " +
		" item_price float, " +
		" item_image_identifier int, " +
		" item_data text, " +
		" item_orders set<FROZEN<item_order>>, " +
		" PRIMARY KEY (item_id) " +
		" );"
	err = session.Query(createItemsCmd).Exec()
	if err != nil {
		log.Println(err)
	}

	// create materialized view for customer balance

	//dropCustomerBalanceIfExistCmd := "DROP MATERIALIZED VIEW IF EXISTS cs5424_groupl.customer_balance;"
	//err = session.Query(dropCustomerBalanceIfExistCmd).Exec()
	//if err != nil {
	//	log.Println(err)
	//	return
	//}

	// Cannot use materialized view in yugabytedb
	//createCustomerBalanceMVCmd := `CREATE MATERIALIZED VIEW cs5424_groupl.customer_balance AS SELECT warehouse_id, district_id, customer_id, balance FROM customer_counters
	//   WHERE c_balance IS NOT NULL AND warehouse_id IS NOT NULL AND district_id IS NOT NULL AND customer_id IS NOT NULL
	//      PRIMARY KEY (warehouse_id, balance, district_id, customer_id)
	//   WITH CLUSTERING ORDER BY (balance DESC);`
	//err = session.Query(createCustomerBalanceMVCmd).Exec()
	//if err != nil {
	//	log.Println(err)
	//	return
	//}
}
