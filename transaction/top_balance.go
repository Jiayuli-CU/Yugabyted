package transaction

import (
	"cs5424project/store/models"
	"cs5424project/store/postgre"
)

func Top10Balance() error {

	db := postgre.GetDB()

	var customers []models.Customer

	err := db.Limit(10).Order("balance desc").Find(&customers).Error

	return err
}