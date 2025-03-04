package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
	"transformer-service/internal/transform"

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
		err := processFunc(msg.Body, queueName) // The processFunc returns transformed data
		if err != nil {
			log.Printf("Error processing message: %v", err)
		}
	}

	return nil
}

func ProcessMessage(body []byte, queueName string) error {
	source := strings.TrimSuffix(queueName, "_bronze")
	transformedData, err := transform.TransformData(body, source)
	if err != nil {
		return fmt.Errorf("failed to transform data: %w", err)
	}

	transformedDataBytes, err := json.Marshal(transformedData)
	if err != nil {
		return fmt.Errorf("failed to marshal transformed data: %w", err)
	}

	silverQueueName := source + "_silver"
	err = PublishToQueue(silverQueueName, transformedDataBytes)
	if err != nil {
		return fmt.Errorf("failed to publish transformed data: %w", err)
	}

	log.Printf("Published transformed data to queue: %s", silverQueueName)
	return nil
}

// PublishToQueue sends a message to RabbitMQ
func PublishToQueue(queueName string, message []byte) error {
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

	// Create a new channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open channel: %v", err)
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare a queue
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Publish the message to the queue
	err = ch.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// log.Printf("Published message to %s", queueName)
	return nil
}
