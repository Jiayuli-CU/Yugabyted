package postgre

import (
	"cs5424project/store/models"
	"errors"
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/sharding"
)

var (
	db               *gorm.DB
	orderCustomerMap = map[uint64]uint64{}
)

const (
	host           = "192.168.48.246"
	port           = "5433"
	user           = "cs5424l"
	password       = "123456"
	dbname         = "yugabyte2"
	shardingNumber = 5
)

func init() {
	//host = os.Getenv("HOST")
	//port = os.Getenv("PORT")
	//user = os.Getenv("USER")
	//password = os.Getenv("PASSWORD")
	//dbname = os.Getenv("DB_NAME")
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
	shardingDB(db)
	//initMigrations(db)
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
	//err = db.AutoMigrate(&models.Customer{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate customer to postgres db: %v\n", err)
	//}
	//err = db.AutoMigrate(&models.Order{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate order to postgres db: %v\n", err)
	//}
	//err = db.AutoMigrate(&models.Item{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate item to postgres db: %v\n", err)
	//}
	err = db.AutoMigrate(&models.OrderLine{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate orderline to postgres db: %v\n", err)
	}
	//err = db.AutoMigrate(&models.Stock{})
	//if err != nil {
	//	log.Fatalf("Fail to auto-migrate stock to postgres db: %v\n", err)
	//}
	log.Printf("Successfully auto-migrated all models to postgres db\n")
}

func shardingAlgorithm(value interface{}) (suffix string, err error) {
	if uid, ok := value.(uint64); ok {
		return fmt.Sprintf("_%02d", uid%shardingNumber), nil
	}
	return "", errors.New("invalid user_id")
}

func shardingDB(db *gorm.DB) {
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "customers"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "customer_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "orders"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:    false,
		ShardingKey:    "order_id",
		NumberOfShards: shardingNumber,
		ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
			if uid, ok := value.(uint64); ok {
				return fmt.Sprintf("_%02d", orderCustomerMap[uid]%shardingNumber), nil
			}
			return "", errors.New("invalid user_id")
		},
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "orderlines"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "items"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "item_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "stocks"))
}

func generateDSN(host, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s", host, user, password, dbname, port)
	//return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s sslrootcert=%s", host, user, password, dbname, port, sslmode, sslrootcert)
}

func GetDB() *gorm.DB {
	return db
}

func GetOrderCustomerMap() map[uint64]uint64 {
	return orderCustomerMap
}

func SetOrderCustomerMap(newMap map[uint64]uint64) {
	orderCustomerMap = newMap
}
