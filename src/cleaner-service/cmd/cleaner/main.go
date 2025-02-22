package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"cleaner-service/internal/clean"
	"cleaner-service/internal/queue"
)

func main() {
	// List of queues to consume from
	// queues := []string{"taxi_trips_raw", "covid_cases_raw", "covid_vulnerability_index_raw", "building_permits_raw", "census_data_raw", "transportation_trips_raw", "public_health_statistics_raw"}
	queues := []string{"taxi_trips_raw", "covid_cases_raw"}

	for _, queueName := range queues {
		go func(queueName string) {
			err := queue.StartConsumer(queueName, processMessage)
			if err != nil {
				log.Fatalf("Failed to start consumer for queue %s: %v", queueName, err)
			}
		}(queueName)
	}

	// Prevent the main function from exiting immediately
	select {}
}

func processMessage(body []byte, queueName string) error {
	source := strings.TrimSuffix(queueName, "_raw")
	cleanedData, err := clean.CleanData(body, source)
	if err != nil {
		return fmt.Errorf("failed to clean data: %w", err)
	}

	cleanedDataBytes, err := json.Marshal(cleanedData)
	if err != nil {
		return fmt.Errorf("failed to marshal cleaned data: %w", err)
	}

	bronzeQueueName := source + "_bronze"
	err = queue.PublishToQueue(bronzeQueueName, cleanedDataBytes)
	if err != nil {
		return fmt.Errorf("failed to publish cleaned data: %w", err)
	}

	log.Printf("Published cleaned data to queue: %s", bronzeQueueName)
	return nil
}
