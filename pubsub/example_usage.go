package pubsub

import (
	"context"
	"fmt"
	"time"
)

// ExampleUsage demonstrates how to use the refactored pubsub factory
func ExampleUsage() {
	// Example 1: Using the builder pattern (recommended)
	config, err := NewConfigBuilder().
		WithPubSubType("kafka").
		WithURL("localhost:9092").
		WithDebug(true).
		WithTrace(false).
		Build()
	if err != nil {
		fmt.Printf("Config validation failed: %v\n", err)
		return
	}

	pubsub, err := NewFactory(config)
	if err != nil {
		fmt.Printf("Failed to create pubsub: %v\n", err)
		return
	}
	defer pubsub.Close()

	// Example 2: Using context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publishing a message
	message := []byte("Hello, World!")
	if err := pubsub.Publish(ctx, "my-topic", message); err != nil {
		fmt.Printf("Failed to publish: %v\n", err)
		return
	}

	// Subscribing to messages
	if err := pubsub.Subscribe(ctx, "my-topic", func(msg string) {
		fmt.Printf("Received message: %s\n", msg)
	}); err != nil {
		fmt.Printf("Failed to subscribe: %v\n", err)
		return
	}

	// Queue subscription with consumer group
	if err := pubsub.QueueSubscribe(ctx, "my-topic", "my-group", func(msg string) {
		fmt.Printf("Queue received message: %s\n", msg)
	}); err != nil {
		fmt.Printf("Failed to queue subscribe: %v\n", err)
		return
	}

	fmt.Println("PubSub operations completed successfully")
}

// ExampleLegacyUsage shows the traditional way (still supported)
func ExampleLegacyUsage() {
	config := FactoryConfig{
		PubsubType: "nats",
		PubsubUrl:  "nats://localhost:4222",
		Debug:      false,
		Trace:      false,
	}

	pubsub, err := NewFactory(config)
	if err != nil {
		fmt.Printf("Failed to create pubsub: %v\n", err)
		return
	}
	defer pubsub.Close()

	ctx := context.Background()

	// Use the pubsub instance...
	if err := pubsub.Publish(ctx, "legacy-topic", []byte("legacy message")); err != nil {
		fmt.Printf("Failed to publish: %v\n", err)
	}
}