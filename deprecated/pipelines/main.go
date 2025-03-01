package main

import (
	"log"
	"sync"
	"encoding/json"

	_ "github.com/lib/pq"
)

var websites = map[string]string{
	"https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500": "taxi_trips",
	"https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=500": "covid_cases",
	"https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500": "covid_vulnerability_index",
	"https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500": "building_permits",
	"https://data.cityofchicago.org/resource/kn9c-c2s2.json?$limit=500": "census_data",
	"https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500": "transportation_trips",
	"https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=500": "public_health_statistics",
}

func main() {
	db, err := connectToPostgres()
	if err != nil {
		log.Fatal("Database connection failed")
	}
	defer db.Close()

	var wg sync.WaitGroup
	ch := make(chan map[string]interface{})

	for url := range websites {
		wg.Add(1)
		go fetchData(url, ch, &wg)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for result := range ch {
		url := result["url"].(string)
		data := result["data"].([]map[string]interface{})
		tableName := websites[url]

		columns := []string{}
		for key := range data[0] {
			columns = append(columns, key)
		}

		err := createTableIfNotExists(db, tableName, columns)
		if err != nil {
			log.Printf("Error creating table: %v", err)
			continue
		}

		var dataRows [][]interface{}
		for _, row := range data {
			var values []interface{}
			for _, col := range columns {
				val := row[col]
		
				// Convert nested maps to JSON strings
				if nestedMap, ok := val.(map[string]interface{}); ok {
					jsonValue, err := json.Marshal(nestedMap)
					if err != nil {
						log.Printf("Error converting nested map to JSON: %v", err)
						jsonValue = []byte("{}")
					}
					val = string(jsonValue)
				}
		
				// Handle slices (arrays) if needed
				if slice, ok := val.([]interface{}); ok {
					jsonValue, err := json.Marshal(slice)
					if err != nil {
						log.Printf("Error converting slice to JSON: %v", err)
						jsonValue = []byte("[]")
					}
					val = string(jsonValue)
				}
		
				values = append(values, val)
			}
			dataRows = append(dataRows, values)
		}
		

		err = insertData(db, tableName, columns, dataRows)
		if err != nil {
			log.Printf("Error inserting data: %v", err)
		}
	}
}
