package postgre

import (
	"cs5424project/store/models"
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

func init() {
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	dbname := os.Getenv("DB_NAME")
	dsn := generateDSN(host, port, user, password, dbname)

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Fail to start postgres db: %v", err)
	}
	conn, _ := db.DB()
	err = conn.Ping()
	if err != nil {
		log.Fatalf("Fail to connect to postgres db: %v", err)
	}
	log.Println("Successfully connected to postgres db")
	initMigrations(db)
}

func initMigrations(db *gorm.DB) {
	err := db.AutoMigrate(&models.Warehouse{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate warehouse to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.District{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate district to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.Customer{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate customer to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.Order{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate order to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.Item{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate item to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.OrderLine{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate orderline to postgres db: %v", err)
	}
	err = db.AutoMigrate(&models.Stock{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate stock to postgres db: %v", err)
	}
	log.Println("Successfully auto-migrated all models to postgres db")
}

func generateDSN(host, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, port, user, password, dbname)
}

func GetDB() *gorm.DB {
	return db
}
