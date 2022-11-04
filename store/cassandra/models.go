package cassandra

import "time"

type Warehouse struct {
	Id                 int
	WarehouseBasicInfo WarehouseBasicInfo
	TaxRate            float32
	YearToDateAmount   float32
}

type WarehouseBasicInfo struct {
	Name    string `cql:"name"`
	Street1 string `cql:"street_1" json:"street_1,omitempty"`
	Street2 string `cql:"street_2" json:"street_2,omitempty"`
	City    string `cql:"city" json:"city,omitempty"`
	State   string `cql:"state" json:"state,omitempty"`
	Zip     string `cql:"zip" json:"zip,omitempty"`
}

type WarehouseCounter struct {
	WarehouseId      int `cql:"warehouse_id" json:"warehouse_id"`
	YearToDateAmount int `cql:"warehouse_year_to_date_payment" json:"warehouse_year_to_date_payment"`
}

type DistrictInfo struct {
	Name    string `cql:"name"`
	Street1 string `cql:"street_1" json:"street_1,omitempty"`
	Street2 string `cql:"street_2" json:"street_2,omitempty"`
	City    string `cql:"city" json:"city,omitempty"`
	State   string `cql:"state" json:"state,omitempty"`
	Zip     string `cql:"zip" json:"zip,omitempty"`
}

type District struct {
	WarehouseId         int                `cql:"warehouse_id" json:"warehouse_id"`
	DistrictId          int                `cql:"district_id" json:"district_id"`
	NextOrderNumber     int                `cql:"next_order_number" json:"next_order_number"`
	WarehouseInfo       WarehouseBasicInfo `cql:"warehouse_address" json:"warehouse_address"`
	DistrictInfo        DistrictInfo       `cql:"district_address" json:"district_address"`
	DistrictTaxRate     float32            `cql:"district_tax_rate" json:"district_tax_rate"`
	WarehouseTaxRate    float32            `cql:"warehouse_tax_rate" json:"warehouse_tax_rate"`
	NextDeliveryOrderId int                `cql:"next_delivery_order_id" json:"next_delivery_order_id"`
}

type OrderLine struct {
	OrderLineId       int    `cql:"order_line_id" json:"order_line_id"`
	ItemId            int    `cql:"item_id" json:"item_id"`
	ItemName          string `cql:"item_name" json:"item_name"`
	AmountInt         int    `cql:"amount" json:"amount"`
	SupplyWarehouseId int    `cql:"supply_warehouse_id" json:"supply_warehouse_id"`
	Quantity          int    `cql:"quantity" json:"quantity"`
	MiscellaneousData string `cql:"miscellaneous_data" json:"miscellaneous_data"`
}

type CustomerInfo struct {
	FirstName   string    `cql:"first_name" json:"first_name,omitempty"`
	MiddleName  string    `cql:"middle_name" json:"middle_name,omitempty"`
	LastName    string    `cql:"last_name" json:"last_name,omitempty"`
	Street1     string    `cql:"street_1" json:"street_1,omitempty"`
	Street2     string    `cql:"street_2" json:"street_2,omitempty"`
	City        string    `cql:"city" json:"city,omitempty"`
	State       string    `cql:"state" json:"state,omitempty"`
	Zip         string    `cql:"zip" json:"zip,omitempty"`
	Phone       string    `cql:"phone" json:"phone,omitempty"`
	Since       time.Time `cql:"since" json:"since,omitempty"`
	Credit      string    `cql:"credit" json:"credit,omitempty"`
	CreditLimit float32   `cql:"credit_limit" json:"credit_limit,omitempty"`
}

type Customer struct {
	WarehouseId       int          `cql:"warehouse_id" json:"warehouse_id"`
	DistrictId        int          `cql:"district_id" json:"district_id"`
	CustomerId        int          `cql:"customer_id" json:"customer_id"`
	BasicInfo         CustomerInfo `cql:"basic_info" json:"basic_info"`
	DiscountRate      float32      `cql:"discount_rate" json:"discount_rate"`
	MiscellaneousData string       `cql:"miscellaneous_data" json:"miscellaneous_data"`
	LastOrderId       int          `cql:"last_order_id" json:"last_order_id"`
}

type CustomerCounter struct {
	PaymentCount      int
	DeliveryCount     int
	Balance           int
	YearToDatePayment int
}

type Order struct {
	WarehouseId  int         `cql:"warehouse_id" json:"warehouse_id"`
	DistrictId   int         `cql:"district_id" json:"district_id"`
	OrderId      int         `cql:"order_id" json:"order_id"`
	CustomerId   int         `cql:"customer_id" json:"customer_id"`
	FirstName    string      `cql:"first_name" json:"first_name,omitempty"`
	MiddleName   string      `cql:"middle_name" json:"middle_name,omitempty"`
	LastName     string      `cql:"last_name" json:"last_name,omitempty"`
	CarrierId    int         `cql:"carrier_id" json:"carrier_id"`
	ItemsNumber  int         `json:"items_number" cql:"items_number"`
	AllLocal     int         `json:"all_local" cql:"all_local"`
	EntryTime    time.Time   `json:"entry_time" cql:"entry_time"`
	OrderLines   []OrderLine `json:"order_lines" cql:"order_lines"`
	DeliveryTime *time.Time  `json:"delivery_time,omitempty" cql:"delivery_time"`
	TotalAmount  int         `json:"total_amount" cql:"total_amount"`
}

type Item struct {
	ItemId              int               `json:"item_id" cql:"item_id"`
	ItemName            string            `json:"item_name" cql:"item_name"`
	ItemPrice           float32           `json:"item_price" cql:"item_price"`
	ItemImageIdentifier int               `json:"item_image_identifier" cql:"item_image_identifier"`
	ItemData            string            `json:"item_data" cql:"item_data"`
	ItemOrders          []OrderCustomerPK `json:"item_orders" cql:"item_orders"`
}

type StockInfo struct {
	District1Info     string `json:"district_1_info" cql:"district_1_info"`
	District2Info     string `json:"district_2_info" cql:"district_2_info"`
	District3Info     string `json:"district_3_info" cql:"district_3_info"`
	District4Info     string `json:"district_4_info" cql:"district_4_info"`
	District5Info     string `json:"district_5_info" cql:"district_5_info"`
	District6Info     string `json:"district_6_info" cql:"district_6_info"`
	District7Info     string `json:"district_7_info" cql:"district_7_info"`
	District8Info     string `json:"district_8_info" cql:"district_8_info"`
	District9Info     string `json:"district_9_info" cql:"district_9_info"`
	District10Info    string `json:"district_10_info" cql:"district_10_info"`
	MiscellaneousData string `cql:"miscellaneous_data" json:"miscellaneous_data"`
}

type Stock struct {
	WarehouseId int       `cql:"warehouse_id" json:"warehouse_id"`
	ItemId      int       `json:"item_id" cql:"item_id"`
	BasicInfo   StockInfo `json:"basic_info" cql:"basic_info"`
}

type StockCounter struct {
	WarehouseId   int `cql:"warehouse_id" json:"warehouse_id"`
	ItemId        int `json:"item_id" cql:"item_id"`
	OrderCount    int `json:"order_count" cql:"order_count"`
	RemoteCount   int `json:"remote_count" cql:"remote_count"`
	Quantity      int `json:"quantity" cql:"quantity"`
	TotalQuantity int `json:"total_quantity" cql:"total_quantity"`
}

//type CustomerPK struct {
//	WarehouseId int `cql:"warehouse_id" json:"warehouse_id"`
//	DistrictId  int `cql:"district_id" json:"district_id"`
//	CustomerId  int `cql:"customer_id" json:"customer_id"`
//}
//
//type OrderPK struct {
//	WarehouseId int `cql:"warehouse_id" json:"warehouse_id"`
//	DistrictId  int `cql:"district_id" json:"district_id"`
//	OrderId     int `cql:"order_id" json:"order_id"`
//}

type OrderCustomerPK struct {
	WarehouseId int `cql:"warehouse_id" json:"warehouse_id"`
	DistrictId  int `cql:"district_id" json:"district_id"`
	OrderId     int `cql:"order_id" json:"order_id"`
	CustomerId  int `cql:"customer_id" json:"customer_id"`
}
