package cassandra

type OrderLine struct {
	OrderLineId       int     `cql:"order_line_id"`
	ItemId            int     `cql:"item_id"`
	ItemName          string  `cql:"item_name"`
	Amount            float64 `cql:"amount"`
	SupplyWarehouseId int     `cql:"supply_warehouse_id"`
	Quantity          int     `cql:"quantity"`
	MiscellaneousData string  `cql:"miscellaneous_data"`
}
