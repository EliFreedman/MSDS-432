package main

import (
	"encoding/json"
	"fetcher-service/internal/fetch"
	"fetcher-service/internal/queue"
	"fmt"
	"log"
	"sync"
)

func main() {

	// Define the limit for the APIs
	limit := 10

	// Dictionary with table names as keys and URLs as values
	urls := map[string]string{
		"taxi_trips":                fmt.Sprintf("https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=%d", limit),
		"covid_cases":               fmt.Sprintf("https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=%d", limit),
		"covid_vulnerability_index": fmt.Sprintf("https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=%d", limit),
		"building_permits":          fmt.Sprintf("https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=%d", limit),
		"census_data":               fmt.Sprintf("https://data.cityofchicago.org/resource/kn9c-c2s2.json?$limit=%d", limit),
		"transportation_trips":      fmt.Sprintf("https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=%d", limit),
		"public_health_statistics":  fmt.Sprintf("https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=%d", limit),
	}

	// Create a channel to receive data from the goroutines
	dataChan := make(chan map[string]interface{})
	var wg sync.WaitGroup

	// Start a new goroutine for each URL
	for tableName, url := range urls {
		wg.Add(1)
		go func(tableName, url string) {
			defer wg.Done()
			ch := make(chan map[string]interface{})
			go fetch.FetchData(url, ch)

			data := <-ch
			data["table_name"] = tableName // Add table name to the data
			dataChan <- data
		}(tableName, url)
	}

	// Collect data from the dataChan and publish to RabbitMQ
	go func() {
		for data := range dataChan {
			log.Printf("Received data for table %s: %v", data["table_name"], data["data"])

			// Convert data to JSON
			message, err := json.Marshal(data)
			if err != nil {
				log.Printf("Failed to marshal data: %v", err)
				continue
			}

			queue_name := data["table_name"].(string) + "_raw"
			log.Printf("Publishing data to queue: %s", queue_name)

			// Publish data to RabbitMQ
			err = queue.PublishToQueue(queue_name, message)
			if err != nil {
				log.Printf("Failed to publish data to queue: %v", err)
			}
		}
	}()

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(dataChan)
	}()

	// Prevent the main function from exiting immediately
	select {}
}
