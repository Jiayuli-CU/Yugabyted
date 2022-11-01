package postgre

import (
	"cs5424project/store/models"
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

const (
	host     = "ap-southeast-1.af363d1a-c9eb-43c7-be5a-06e807e2467b.aws.ybdb.io"
	port     = "5433"
	user     = "admin"
	password = "A6jDGDt2TwS3Etk3QuDo3hp89iwPvD"
	dbname   = "yugabyte"
)

func init() {

	dsn := generateDSN(host, port, user, password, dbname)

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Fail to start postgres db: %v\n", err)
	}
	conn, _ := db.DB()
	err = conn.Ping()
	if err != nil {
		log.Fatalf("Fail to connect to postgres db: %v\n", err)
	}
	log.Printf("Successfully connected to postgres db\n")
	initMigrations(db)
}

func initMigrations(db *gorm.DB) {
	var err error
	//err = db.AutoMigrate(&models.Warehouse{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate warehouse to postgres db: %v\n", err)
	//}
	//err = db.AutoMigrate(&models.District{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate district to postgres db: %v\n", err)
	//}
	err = db.AutoMigrate(&models.Customer{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate customer to postgres db: %v\n", err)
	}
	//err = db.AutoMigrate(&models.Order{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate order to postgres db: %v\n", err)
	//}
	//err = db.AutoMigrate(&models.Item{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate item to postgres db: %v\n", err)
	//}
	//err = db.AutoMigrate(&models.OrderLine{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate orderline to postgres db: %v\n", err)
	//}
	err = db.AutoMigrate(&models.Stock{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate stock to postgres db: %v\n", err)
	}
	log.Printf("Successfully auto-migrated all models to postgres db\n")
}

func generateDSN(host, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s", host, user, password, dbname, port)
	//return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s sslrootcert=%s", host, user, password, dbname, port, sslmode, sslrootcert)
}

func GetDB() *gorm.DB {
	return db
}
