package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"storage-service/internal/db"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

// RabbitMQ connection URL
const RabbitMQURL = "amqp://guest:guest@rabbitmq:5672/"
const maxRetries = 5
const retryInterval = 5 * time.Second

// StartConsumer listens for messages and processes them
func StartConsumer(queueName string, processFunc func([]byte, string) error) error {
	var conn *amqp.Connection
	var err error

	// Retry mechanism for connecting to RabbitMQ
	for i := 0; i < maxRetries; i++ {
		conn, err = amqp.Dial(RabbitMQURL)
		if err == nil {
			log.Printf("Successfully connected to RabbitMQ on attempt %d", i+1)
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(retryInterval)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w", maxRetries, err)
	}
	defer conn.Close()

	// Create a channel over the connection
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
		return err
	}
	defer ch.Close()

	// Create a consumer to receive messages
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
		return err
	}

	// Process incoming messages
	log.Printf("Waiting for messages in queue: %s", queueName)
	for msg := range msgs {
		log.Printf("Received message from source: %s", queueName)
		err := processFunc(msg.Body, queueName) // The processFunc sends data to Postgres
		if err != nil {
			log.Printf("Error processing message: %v", err)
		}
	}

	return nil
}

// ProcessMessage processes a message from the queue
func ProcessMessage(body []byte, queueName string) error {
	source := strings.TrimSuffix(queueName, "_silver")

	// Unmarshal the records from the message body
	var records []map[string]interface{}
	err := json.Unmarshal(body, &records)
	if err != nil {
		return fmt.Errorf("error parsing JSON records: %v", err)
	}

	// Ensure we have at least one record to infer schema
	if len(records) == 0 {
		return fmt.Errorf("error: no records received")
	}

	// Infer schema from the first record
	schema := make(map[string]string)
	for key, value := range records[0] {
		switch value.(type) {
		case int, int32, int64:
			schema[key] = "INTEGER"
		case float32, float64:
			schema[key] = "REAL"
		case string:
			schema[key] = "TEXT"
		case bool:
			schema[key] = "BOOLEAN"
		default:
			schema[key] = "TEXT" // Default to TEXT for unrecognized types
		}
	}

	// Convert schema to JSON
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("error marshaling schema to JSON: %v", err)
	}

	// Create table using inferred schema
	if err := db.CreateTable(source, string(schemaJSON)); err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add records to the table
	if err := db.AddRecords(source, records); err != nil {
		return fmt.Errorf("error inserting records: %v", err)
	}

	return nil
}
