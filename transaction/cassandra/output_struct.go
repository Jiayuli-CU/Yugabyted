package cassandra

import (
	"cs5424project/store/cassandra"
	"time"
)

var outputButton = true

type CustomerInfoForNewOrder struct {
	CustomerIdentifier CustomerIdentifier `json:"customer_identifier"`
	LastName           string             `json:"last_name,omitempty"`
	Credit             string             `json:"credit,omitempty"`
	Discount           float32            `json:"discount,omitempty"`
}

type CustomerInfoForPayment struct {
	CustomerIdentifier CustomerIdentifier     `json:"customer_identifier"`
	CustomerBasicInfo  cassandra.CustomerInfo `json:"customer_basic_info"`
	Discount           float32                `json:"discount"`
	Balance            float32                `json:"balance"`
}

type ItemInfo struct {
	ItemNumber          int     `json:"item_number,omitempty"`
	ItemName            string  `json:"item_name,omitempty"`
	SupplierWarehouseId int     `json:"supplier_warehouse_id,omitempty"`
	Quantity            int     `json:"quantity,omitempty"`
	OrderLineAmount     float32 `json:"order_line_amount,omitempty"`
	StockQuantity       int     `json:"stock_quantity,omitempty"`
}

type NewOrderTransactionOutput struct {
	TransactionType  string                  `json:"transaction_type,omitempty"`
	CustomerInfo     CustomerInfoForNewOrder `json:"customer_info"`
	WarehouseTaxRate float32                 `json:"warehouse_tax_rate,omitempty"`
	DistrictTaxRate  float32                 `json:"district_tax_rate,omitempty"`
	OrderNumber      int                     `json:"order_number,omitempty"`
	EntryDate        time.Time               `json:"entry_date"`
	ItemNumbers      int                     `json:"item_numbers,omitempty"`
	TotalAmount      float32                 `json:"total_amount,omitempty"`
	ItemInfo         []ItemInfo              `json:"item_info"`
}

type OrderInfo struct {
	WarehouseId int
	DistrictId  int
	OrderId     int
	CustomerId  int
	OrderLines  []cassandra.OrderLine
}

type CustomerIdentifier struct {
	WarehouseId int `json:"warehouse_id"`
	DistrictId  int `json:"district_id"`
	CustomerId  int `json:"customer_id"`
}

type PaymentTransactionOutput struct {
	TransactionType  string                       `json:"transaction_type,omitempty"`
	CustomerInfo     CustomerInfoForPayment       `json:"customer_info"`
	WarehouseAddress cassandra.WarehouseBasicInfo `json:"warehouse_address"`
	DistrictAddress  cassandra.DistrictInfo       `json:"district_address"`
	Payment          float32                      `json:"payment,omitempty"`
}

type OrderStatusItemInfo struct {
	ItemId              int       `json:"item_id,omitempty"`
	SupplierWarehouseId int       `json:"supplier_warehouse_id,omitempty"`
	Quantity            int       `json:"quantity,omitempty"`
	Amount              float32   `json:"amount,omitempty"`
	DeliveryDate        time.Time `json:"delivery_date"`
}

type OrderStatusTransactionOutput struct {
	TransactionType string                `json:"transaction_type,omitempty"`
	FirstName       string                `json:"first_name,omitempty"`
	MiddleName      string                `json:"middle_name,omitempty"`
	LastName        string                `json:"last_name,omitempty"`
	Balance         float32               `json:"balance,omitempty"`
	LastOrderId     int                   `json:"last_order_id,omitempty"`
	EntryDate       time.Time             `json:"entry_date"`
	CarrierId       int                   `json:"carrier_id,omitempty"`
	Items           []OrderStatusItemInfo `json:"items,omitempty"`
}

type TopBalanceTransactionOutput struct {
	TransactionType string  `json:"transaction_type,omitempty"`
	FirstName       string  `json:"first_name,omitempty"`
	MiddleName      string  `json:"middle_name,omitempty"`
	LastName        string  `json:"last_name,omitempty"`
	Balance         float32 `json:"balance,omitempty"`
	WarehouseName   string  `json:"warehouse_name,omitempty"`
	DistrictName    string  `json:"district_name,omitempty"`
}

type PopularItemTransactionOutput struct {
	TransactionType            string                               `json:"transaction_type,omitempty"`
	WarehouseId                int                                  `json:"warehouse_id,omitempty"`
	DistrictId                 int                                  `json:"district_id,omitempty"`
	NumberOfOrdersToBeExamined int                                  `json:"number_of_orders_to_be_examined,omitempty"`
	OrderInfos                 []OrderInfoForPopularItemTransaction `json:"order_infos,omitempty"`
	PopularItemPercentages     []PopularItemPercentage              `json:"popular_item_percentages,omitempty"`
}

type RelatedCustomerTransactionOutput struct {
	TransactionType            string   `json:"transaction_type,omitempty"`
	RelatedCustomerIdentifiers []string `json:"related_customer_identifiers,omitempty"`
}

type OrderInfoForPopularItemTransaction struct {
	OrderId                  int                                 `json:"order_id,omitempty"`
	EntryTime                time.Time                           `json:"entry_time,omitempty"`
	FirstName                string                              `json:"first_name,omitempty"`
	MiddleName               string                              `json:"middle_name,omitempty"`
	LastName                 string                              `json:"last_name,omitempty"`
	PopularItemsForThisOrder []ItemInfoForPopularItemTransaction `json:"popular_items_for_this_order,omitempty"`
}

type ItemInfoForPopularItemTransaction struct {
	ItemName string `json:"item_name,omitempty"`
	Quantity int    `json:"quantity,omitempty"`
}

type PopularItemPercentage struct {
	ItemName   string  `json:"item_name,omitempty"`
	Percentage float32 `json:"percentage,omitempty"`
}
