package postgre

import (
	"cs5424project/store/models"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/sharding"
)

var (
	db               *gorm.DB
	orderCustomerMap = map[uint64]uint64{}
)

const (
	//host                   = "192.168.48.246"
	//port                   = "5433"
	//user                   = "cs5424l"
	//password               = "123456"
	//dbname                 = "yugabyte2"
	shardingNumber         = 5
	maxConnectionPoolCount = 500
	maxConnectionCount     = 2500
)

func initDB(useMigration bool) {
	cfgFile, err := os.Open("./config.json")
	if err != nil {
		log.Fatalf("Open config file error: %v\n", err)
	}
	defer cfgFile.Close()
	byteValue, err := ioutil.ReadAll(cfgFile)
	if err != nil {
		log.Fatalf("Read config file error: %v\n", err)
	}
	data := struct {
		Host     string
		Port     string
		User     string
		Password string
		DBName   string
	}{}
	if err = json.Unmarshal(byteValue, &data); err != nil {
		log.Fatalf("Unmarshal config data error: %v\n", err)
	}
	dsn := generateDSN(data.Host, data.Port, data.User, data.Password, data.DBName)

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
	conn.SetMaxIdleConns(maxConnectionPoolCount)
	conn.SetMaxOpenConns(maxConnectionCount)
	conn.SetConnMaxLifetime(time.Hour)
	//shardingDB(db)
	if useMigration {
		InitMigrations(db)
	}
}

func InitMigrations(db *gorm.DB) {
	var err error
	err = db.AutoMigrate(&models.Warehouse{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate warehouse to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.District{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate district to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.Customer{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate customer to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.Order{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate order to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.Item{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate item to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.OrderLine{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate orderline to postgres db: %v\n", err)
	}
	err = db.AutoMigrate(&models.Stock{})
	if err != nil {
		log.Fatalf("Fail to auto-migrate stock to postgres db: %v\n", err)
	}
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
		ShardingKey:         "warehouse_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "customers"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "warehouse_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "orders"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "warehouse_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "order_lines"))
	db.Use(sharding.Register(sharding.Config{
		DoubleWrite:         false,
		ShardingKey:         "warehouse_id",
		NumberOfShards:      shardingNumber,
		ShardingAlgorithm:   shardingAlgorithm,
		PrimaryKeyGenerator: sharding.PKSnowflake,
	}, "stocks"))
}

func generateDSN(host, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s", host, user, password, dbname, port)
	//return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s sslrootcert=%s", host, user, password, dbname, port, sslmode, sslrootcert)
}

func GetDB(useMigration bool) *gorm.DB {
	if db == nil {
		initDB(true)
	}
	return db
}

func GetOrderCustomerMap() map[uint64]uint64 {
	return orderCustomerMap
}

func SetOrderCustomerMap(newMap map[uint64]uint64) {
	orderCustomerMap = newMap
}
