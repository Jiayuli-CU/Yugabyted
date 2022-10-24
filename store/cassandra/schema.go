package cassandra

func createSchema() {

	districtQuery := "CREATE TABLE IF NOT EXISTS cs5424_groupI.districts " +
		"(district_id int, warehouse_id int, name text, street_line_1 text, street_line_2 text, city text, state text, zip text, tax_rate float, year_to_date_amount float,next_available_order_number int, PRIMARY KEY (id));"

}
