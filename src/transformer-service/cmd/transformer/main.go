package main

import (
	"log"

	"transformer-service/internal/queue"
)

func main() {
	// List of queues to consume from
	queues := []string{"taxi_trips_bronze", "covid_cases_bronze", "covid_vulnerability_index_bronze", "building_permits_bronze", "census_data_bronze", "transportation_trips_bronze", "public_health_statistics_bronze"}

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
