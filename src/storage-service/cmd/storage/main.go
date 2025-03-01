package main

import (
	"log"

	"storage-service/internal/db"
	"storage-service/internal/queue"
)

func main() {
	// Establish a persistent database connection
	err := db.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close() // Ensure it closes when main exits

	// List of queues to consume from
	queues := []string{"taxi_trips_silver", "covid_cases_silver", "covid_vulnerability_index_silver", "building_permits_silver", "census_data_silver", "transportation_trips_silver", "public_health_statistics_silver"}

	for _, queueName := range queues {
		go func(queueName string) {
			for {
				err := queue.StartConsumer(queueName, queue.ProcessMessage)
				if err != nil {
					log.Printf("Waiting for queue %s to appear: %v", queueName, err)
				} else {
					break
				}
			}
		}(queueName)
	}

	// Prevent the main function from exiting immediately
	select {}
}
