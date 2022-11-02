package models

import "time"

type Warehouse struct {
	Id               uint64  `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	Name             string  `gorm:"type:varchar(10);column:name;unique;not null"`
	StreetLine1      string  `gorm:"type:varchar(20);column:street_line_1;not null"`
	StreetLine2      string  `gorm:"type:varchar(20);column:street_line_2;not null"`
	City             string  `gorm:"type:varchar(20);column:city;not null"`
	State            string  `gorm:"type:char(2);column:state;not null"`
	Zip              string  `gorm:"type:char(9);column:zip;not null"`
	TaxRate          float64 `gorm:"type:decimal(4,4);column:tax_rate;not null"`
	YearToDateAmount float64 `gorm:"type:decimal(12,2);column:year_to_date_amount;not null"`
}

type District struct {
	Id uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	//Warehouse                Warehouse `gorm:"foreignKey:WarehouseId;references:id"`
	WarehouseId              uint64  `gorm:"primaryKey;autoIncrement:false;type:int;column:warehouse_id;not null"`
	Name                     string  `gorm:"type:varchar(10);column:name;unique;not null"`
	StreetLine1              string  `gorm:"type:varchar(20);column:street_line_1;not null"`
	StreetLine2              string  `gorm:"type:varchar(20);column:street_line_2;not null"`
	City                     string  `gorm:"type:varchar(20);column:city;not null"`
	State                    string  `gorm:"type:char(2);column:state;not null"`
	Zip                      string  `gorm:"type:char(9);column:zip;not null"`
	TaxRate                  float64 `gorm:"type:decimal(4,4);column:tax_rate;not null"`
	YearToDateAmount         float64 `gorm:"type:decimal(12,2);column:year_to_date_amount;not null"`
	NextAvailableOrderNumber uint64  `gorm:"type:int;column:next_available_order_number;not null"`
}

type Customer struct {
	Id uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	//Warehouse         Warehouse `gorm:"foreignKey:WarehouseId;references:id"`
	WarehouseId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:warehouse_id;not null"`
	//District          District  `gorm:"foreignKey:DistrictId;references:id"`
	DistrictId        uint64    `gorm:"primaryKey;autoIncrement:false;type:int;column:district_id;not null"`
	FirstName         string    `gorm:"type:varchar(16);column:first_name;not null"`
	MiddleName        string    `gorm:"type:char(2);column:middle_name"`
	LastName          string    `gorm:"type:varchar(16);column:last_name;not null"`
	StreetLine1       string    `gorm:"type:varchar(20);column:street_line_1;not null"`
	StreetLine2       string    `gorm:"type:varchar(20);column:street_line_2;not null"`
	City              string    `gorm:"type:varchar(20);column:city;not null"`
	State             string    `gorm:"type:char(2);column:state;not null"`
	Zip               string    `gorm:"type:char(9);column:zip;not null"`
	Phone             string    `gorm:"type:char(16);column:phone;not null"`
	CreateTime        time.Time `gorm:"type:timestamp;column:create_time;not null"`
	CreditStatus      string    `gorm:"type:char(2);column:credit_status;not null"`
	CreditLimit       float64   `gorm:"type:decimal(12,2);column:credit_limit;not null"`
	DiscountRate      float64   `gorm:"type:decimal(4,4);column:discount_rate;not null"`
	Balance           float64   `gorm:"type:decimal(12,2);column:balance;not null"`
	YearToDatePayment float64   `gorm:"type:float;column:year_to_date_payment;not null"`
	PaymentsNumber    uint64    `gorm:"type:int;column:payments_number;not null"`
	DeliveriesNumber  uint64    `gorm:"type:int;column:deliveries_number;not null"`
	MiscellaneousData string    `gorm:"type:varchar(500);column:miscellaneous_data"`
}

type Order struct {
	Id uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	//Warehouse   Warehouse `gorm:"foreignKey:WarehouseId;references:id"`
	WarehouseId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:warehouse_id;not null"`
	//District    District  `gorm:"foreignKey:DistrictId;references:id"`
	DistrictId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:district_id;not null"`
	//Customer    Customer  `gorm:"foreignKey:CustomerId;references:id"`
	CustomerId  uint64    `gorm:"type:int;column:customer_id;not null"`
	CarrierId   uint64    `gorm:"type:int;column:carrier_id;not null"`
	ItemsNumber uint64    `gorm:"type:int;column:items_number;not null"`
	Status      int       `gorm:"type:decimal(1,0);column:status;not null"`
	EntryTime   time.Time `gorm:"type:timestamp;column:entry_time;not null"`
}

type Item struct {
	Id      uint64  `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	Name    string  `gorm:"type:varchar(24);column:name;unique;not null"`
	Price   float64 `gorm:"type:decimal(5,2);column:price;not null"`
	ImageId uint64  `gorm:"type:int;column:image_id;not null"`
	Data    string  `gorm:"type:varchar(50);column:data"`
}

type OrderLine struct {
	Id uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:id"`
	//Warehouse         Warehouse `gorm:"foreignKey:WarehouseId;references:id"`
	WarehouseId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:warehouse_id;not null"`
	//District          District  `gorm:"foreignKey:DistrictId;references:id"`
	DistrictId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:district_id;not null"`
	//Order             Order     `gorm:"foreignKey:OrderId;references:id"`
	OrderId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:order_id;not null"`
	//Item              Item      `gorm:"foreignKey:ItemId;references:id"`
	ItemId            uint64    `gorm:"type:int;column:item_id;not null"`
	DeliveryTime      time.Time `gorm:"type:timestamp;column:delivery_time"`
	Price             float64   `gorm:"type:decimal(7,2);column:price;not null"`
	SupplyNumber      uint64    `gorm:"type:int;column:supply_number;not null"`
	Quantity          int       `gorm:"type:decimal(2,0);column:quantity;not null"`
	MiscellaneousData string    `gorm:"type:char(24);column:miscellaneous_data;not null"`
}

type Stock struct {
	//Warehouse                 Warehouse `gorm:"foreignKey:WarehouseId;references:id"`
	WarehouseId uint64 `gorm:"primaryKey;autoIncrement:false;type:int;column:warehouse_id;not null"`
	//Item                      Item      `gorm:"foreignKey:ItemId;references:id"`
	ItemId                    uint64  `gorm:"primaryKey;autoIncrement:false;type:int;column:item_id;not null"`
	Quantity                  int     `gorm:"type:decimal(4,0);column:quantity;not null"`
	YearToDateQuantityOrdered float64 `gorm:"type:decimal(8,2);column:year_to_date_quantity_ordered;not null"`
	OrdersNumber              uint64  `gorm:"type:int;column:orders_number;not null"`
	RemoteOrdersNumber        uint64  `gorm:"type:int;column:remote_orders_number;not null"`
	District1Info             string  `gorm:"type:char(24);column:district_1_info;not null"`
	District2Info             string  `gorm:"type:char(24);column:district_2_info;not null"`
	District3Info             string  `gorm:"type:char(24);column:district_3_info;not null"`
	District4Info             string  `gorm:"type:char(24);column:district_4_info;not null"`
	District5Info             string  `gorm:"type:char(24);column:district_5_info;not null"`
	District6Info             string  `gorm:"type:char(24);column:district_6_info;not null"`
	District7Info             string  `gorm:"type:char(24);column:district_7_info;not null"`
	District8Info             string  `gorm:"type:char(24);column:district_8_info;not null"`
	District9Info             string  `gorm:"type:char(24);column:district_9_info;not null"`
	District10Info            string  `gorm:"type:char(24);column:district_10_info;not null"`
	MiscellaneousData         string  `gorm:"type:varchar(50);column:miscellaneous_data"`
}
