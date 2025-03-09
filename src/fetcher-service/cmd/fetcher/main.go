package main

import (
	"encoding/json"
	"fetcher-service/internal/fetch"
	"fetcher-service/internal/queue"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {

	// Define the limit for the APIs
	limit := 500

	// Dictionary with table names as keys and base URLs as values
	baseURLs := map[string]string{
		"taxi_trips":                "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=%d&$offset=%d",
		"covid_cases":               "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=%d&$offset=%d",
		"building_permits":          "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=%d&$offset=%d",
		"transportation_trips":      "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=%d&$offset=%d",
	}

	onceURLs := map[string]string{
		"covid_vulnerability_index": "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=%d&$offset=%d",
		"census_data":               "https://data.cityofchicago.org/resource/kn9c-c2s2.json?$limit=%d&$offset=%d",
		"public_health_statistics":  "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=%d&$offset=%d",
	}

	// This only loops once
	// Create a channel to receive data from the goroutines
	dataChan := make(chan map[string]interface{})
	var wg sync.WaitGroup

	// Start a new goroutine for each URL
	for tableName, baseURL := range onceURLs {
		offset := 0 // Start from the first page
		wg.Add(1)
		go func(tableName, baseURL string) {
			defer wg.Done()
			ch := make(chan map[string]interface{})
			// Construct the URL with the current offset
			url := fmt.Sprintf(baseURL, limit, offset)
			go fetch.FetchData(url, ch)

			data := <-ch
			data["table_name"] = tableName // Add table name to the data
			dataChan <- data

			// Increment the offset for the next page
			offset += limit
		}(tableName, baseURL)
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

	// Repeat the process 5 times
	for i := 0; i < 20; i++ {
		// Create a channel to receive data from the goroutines
		dataChan := make(chan map[string]interface{})
		var wg sync.WaitGroup

		// Start a new goroutine for each URL
		for tableName, baseURL := range baseURLs {
			offset := 0 // Start from the first page
			wg.Add(1)
			go func(tableName, baseURL string) {
				defer wg.Done()
				ch := make(chan map[string]interface{})
				// Construct the URL with the current offset
				url := fmt.Sprintf(baseURL, limit, offset)
				go fetch.FetchData(url, ch)

				data := <-ch
				data["table_name"] = tableName // Add table name to the data
				dataChan <- data

				// Increment the offset for the next page
				offset += limit
			}(tableName, baseURL)
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

		time.Sleep(time.Second * 60)
	}
}
