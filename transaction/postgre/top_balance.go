package postgre

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
	"log"
)

func Top10Balance() error {
	db := postgre.GetDB(false)
	var customers []models.Customer

	err := db.Limit(10).Order("balance desc").Find(&customers).Error

	if err != nil {
		log.Printf("Top balance transaction error: %v", err)
	}
	return err
}
