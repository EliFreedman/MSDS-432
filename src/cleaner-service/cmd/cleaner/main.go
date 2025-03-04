package main

import (
	"log"
	"time"

	"cleaner-service/internal/queue"
)

func main() {
	// List of queues to consume from
	queues := []string{
		"taxi_trips_raw",
		"covid_cases_raw",
		"covid_vulnerability_index_raw",
		"building_permits_raw",
		"census_data_raw",
		"transportation_trips_raw",
		"public_health_statistics_raw",
	}

	for _, queueName := range queues {
		go func(queueName string) {
			for {
				err := queue.StartConsumer(queueName, queue.ProcessMessage)
				if err != nil {
					log.Printf("Waiting for queue %s to appear: %v", queueName, err)
					time.Sleep(15 * time.Second)
				} else {
					break
				}
			}
		}(queueName)
	}

	// Prevent the main function from exiting immediately
	select {}
}
