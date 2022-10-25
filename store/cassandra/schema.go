package cassandra

import (
	"log"
)

func createSchema() {

	var err error
	warehouseAddressType := "CREATE TYPE IF NOT EXISTS cs5424_groupI.warehouse_info " +
		"(name text, street_1 text, street_2 text, city text, state text, zip text);"
	err = session.Query(warehouseAddressType).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	districtAddressType := "CREATE TYPE IF NOT EXISTS cs5424_groupI.district_info " +
		"(name text, street_1 text, street_2 text, city text, state text, zip text);"
	err = session.Query(districtAddressType).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	districtQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.districts " +
		"(warehouse_id int, district_id int, next_order_number int, district_address FROZEN<district_info>, district_tax_rate float, warehouse_address FROZEN<warehouse_info> static, warehouse_tax_rate float static, " +
		"PRIMARY KEY ((warehouse_id), district_id));"
	err = session.Query(districtQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	customerInfoType := "CREATE TYPE IF NOT EXISTS cs5424_groupI.customer_info " +
		"(first text, middle text, last text, street_1 text, street_2 text, city text, state text, zip text, phone text, since timestamp, credit text, credit_limit float);"
	err = session.Query(customerInfoType).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	customerQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.customers " +
		"(warehouse_id int, district_id int, customer_id int, basic_info FROZEN<customer_info>, discount_rate float, balance float, year_to_date_payment float, miscellaneous_data text, " +
		"PRIMARY KEY ((warehouse_id, district_id), customer_id));"
	err = session.Query(customerQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	customerCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.customer_counters " +
		"(warehouse_id int, district_id int, customer_id int, payment_count counter, delivery_count counter, " +
		"PRIMARY KEY ((warehouse_id, district_id), customer_id));"
	err = session.Query(customerCounterQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	orderLineTypeQuery := "CREATE TYPE IF NOT EXISTS cs5424_groupI.order_line " +
		"(order_line_id int, item_id int, amount float, supply_warehouse_id int, quantity int, miscellaneous_data text);"
	err = session.Query(orderLineTypeQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	orderQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.orders " +
		"(warehouse_id int, district_id int, order_id int, customer_id int, carrier_id int, items_number int, all_local int, entry_time timestamp, order_lines set<FROZEN<order_line>>, delivery_time timestamp, " +
		"PRIMARY KEY ((warehouse_id, district_id), order_id));"
	err = session.Query(orderQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	stockInfoTypeQuery := "CREATE TYPE IF NOT EXISTS cs5424_groupI.stock_info " +
		"(district_1_info text, district_2_info text, district_3_info text,district_4_info text, district_5_info text, district_6_info text, district_7_info text, district_8_info text, district_9_info text, district_10_info text, miscellaneous_data text);"
	err = session.Query(stockInfoTypeQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	stockQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.stocks " +
		"(warehouse_id int, item_id int, basic_info FROZEN<stock_info>, " +
		"PRIMARY KEY ((warehouse_id), item_id));"
	err = session.Query(stockQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

	stockCounterQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.stock_counter " +
		"(warehouse_id int, item_id int, order_count counter, remote_count counter, quantity counter, total_quantity counter " +
		"PRIMARY KEY ((warehouse_id), item_id));"
	err = session.Query(stockCounterQuery).Exec()
	if err != nil {
		log.Println(err)
		return
	}

}
