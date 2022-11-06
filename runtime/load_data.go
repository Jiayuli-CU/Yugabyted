package runtime

import (
	"cs5424project/data"
	"log"
)

func LoadDataToDB() {
	err := data.LoadWarehouse()
	if err != nil {
		log.Fatalf("Load warehouses error: %v\n", err)
	}
	err = data.LoadOrder()
	if err != nil {
		log.Fatalf("Load orders error: %v\n", err)
	}
	err = data.LoadStock()
	if err != nil {
		log.Fatalf("Load stocks error: %v\n", err)
	}
	err = data.LoadDistrict()
	if err != nil {
		log.Fatalf("Load districts error: %v\n", err)
	}
	err = data.LoadCustomer()
	if err != nil {
		log.Fatalf("Load customers error: %v\n", err)
	}
	err = data.LoadItem()
	if err != nil {
		log.Fatalf("Load items error: %v\n", err)
	}
	err = data.LoadOrderLine()
	if err != nil {
		log.Fatalf("Load orderlines error: %v\n", err)
	}
}
