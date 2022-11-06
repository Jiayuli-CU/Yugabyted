package runtime

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

func Config(host, port, user, password, dbname string) {

	cfgPath := "./config.json"
	cfgFile, err := os.Create(cfgPath)
	if err != nil {
		log.Fatalf("Create config file error: %v\n", err)
	}
	defer cfgFile.Close()

	data := struct {
		Host     string
		Port     string
		User     string
		Password string
		DBName   string
	}{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		DBName:   dbname,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Marshal data to json error: %v\n", err)
	}
	err = ioutil.WriteFile(cfgPath, jsonData, 0644)
	if err != nil {
		log.Fatalf("Write to json file error: %v\n", err)
	}
}
