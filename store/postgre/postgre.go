package postgre

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	host     = ""
	port     = ""
	user     = ""
	password = ""
	dbname   = ""
)

var db *gorm.DB

func init() {
	var err error
	dsn := generateDSN(host, port, user, password, dbname)
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("fail to start postgre db")
	}
}

func generateDSN(host, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, port, user, password, dbname)
}

func GetDB() *gorm.DB {
	return db
}
