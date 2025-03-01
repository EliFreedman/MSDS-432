package main

import (
	"log"
	"time"

	"cleaner-service/internal/queue"
)

func main() {
	// List of queues to consume from
	queues := []string{
		// "taxi_trips_raw", 				// Working
		// "covid_cases_raw", 				// Working
		// "covid_vulnerability_index_raw", // Working
		// "building_permits_raw", 			// Working
		// "census_data_raw",				// Working
		// "transportation_trips_raw",		// Working
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
