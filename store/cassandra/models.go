package cassandra

import "time"

type OrderLine struct {
	OrderLineId       int     `cql:"order_line_id"`
	ItemId            int     `cql:"item_id"`
	ItemName          string  `cql:"item_name"`
	Amount            float64 `cql:"amount"`
	SupplyWarehouseId int     `cql:"supply_warehouse_id"`
	Quantity          int     `cql:"quantity"`
	MiscellaneousData string  `cql:"miscellaneous_data"`
}

type CustomerInfo struct {
	FirstName   string    `cql:"first" json:"first_name,omitempty"`
	MiddleName  string    `cql:"middle" json:"middle_name,omitempty"`
	LastName    string    `cql:"last" json:"last_name,omitempty"`
	Street1     string    `cql:"street_1" json:"street_1,omitempty"`
	Street2     string    `cql:"street_2" json:"street_2,omitempty"`
	City        string    `cql:"city" json:"city,omitempty"`
	State       string    `cql:"state" json:"state,omitempty"`
	Zip         string    `cql:"zip" json:"zip,omitempty"`
	Phone       string    `cql:"phone" json:"phone,omitempty"`
	Since       time.Time `cql:"since" json:"since,omitempty"`
	Credit      string    `cql:"credit" json:"credit,omitempty"`
	CreditLimit float32   `cql:"credit_limit" json:"creditLimit,omitempty"`
}
