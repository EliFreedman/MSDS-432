package queue

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// RabbitMQConfig holds the configuration for RabbitMQ
const RabbitMQURL = "amqp://guest:guest@rabbitmq:5672/"
const maxRetries = 5
const retryInterval = 5 * time.Second

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

	log.Printf("Published message to %s", queueName)
	return nil
}
