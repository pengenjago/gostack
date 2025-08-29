package pubsub

import (
	"context"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
)

// PubsubEventHandler defines the function signature for handling pubsub events
type PubsubEventHandler func(msg string)

// PubSub combines both Publisher and Subscriber interfaces
type PubSub interface {
	Publisher
	Subscriber
}

// Publisher defines the interface for publishing messages
type Publisher interface {
	// Publish sends a message to the specified topic
	Publish(ctx context.Context, topic string, msg []byte) error
	// Close closes the publisher and releases resources
	Close() error
}

// Subscriber defines the interface for subscribing to messages
type Subscriber interface {
	// Subscribe subscribes to a topic with the given event handler
	Subscribe(ctx context.Context, topic string, eventHandler PubsubEventHandler) error
	// QueueSubscribe subscribes to a topic with queue group semantics
	QueueSubscribe(ctx context.Context, topic, group string, eventHandler PubsubEventHandler) error
	// Close closes the subscriber and releases resources
	Close() error
}

// FactoryConfig holds the configuration for creating a PubSub instance
type FactoryConfig struct {
	// PubsubType specifies the pubsub implementation type (kafka, nats, rabbitmq, redis)
	PubsubType string
	// PubsubUrl is the connection URL for the pubsub service
	// Examples: localhost:9092, nats://localhost:4222, amqp://localhost:5672
	PubsubUrl string
	// Debug enables debug logging (default: false)
	Debug bool
	// Trace enables trace logging (default: false)
	Trace bool
}

// Validate checks if the FactoryConfig is valid and returns an error if not
func (c *FactoryConfig) Validate() error {
	if strings.TrimSpace(c.PubsubType) == "" {
		return ErrMissingPubSubType
	}

	if strings.TrimSpace(c.PubsubUrl) == "" {
		return ErrMissingPubSubURL
	}

	// Validate that the pubsub type is supported
	if !isValidPubSubType(c.PubsubType) {
		return ErrUnsupportedPubSubType
	}

	return nil
}

// isValidPubSubType checks if the given pubsub type is supported
func isValidPubSubType(pubsubType string) bool {
	switch TypePubsub(strings.ToLower(pubsubType)) {
	case Kafka, NATS, RabbitMQ, Redis:
		return true
	default:
		return false
	}
}

// ConfigBuilder provides a fluent interface for building FactoryConfig
type ConfigBuilder struct {
	config FactoryConfig
}

// NewConfigBuilder creates a new ConfigBuilder instance
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{}
}

// WithPubSubType sets the pubsub type (kafka, nats, rabbitmq, redis)
func (b *ConfigBuilder) WithPubSubType(pubsubType string) *ConfigBuilder {
	b.config.PubsubType = pubsubType
	return b
}

// WithURL sets the pubsub connection URL
func (b *ConfigBuilder) WithURL(url string) *ConfigBuilder {
	b.config.PubsubUrl = url
	return b
}

// WithDebug enables or disables debug logging
func (b *ConfigBuilder) WithDebug(debug bool) *ConfigBuilder {
	b.config.Debug = debug
	return b
}

// WithTrace enables or disables trace logging
func (b *ConfigBuilder) WithTrace(trace bool) *ConfigBuilder {
	b.config.Trace = trace
	return b
}

// Build creates and validates the FactoryConfig
func (b *ConfigBuilder) Build() (FactoryConfig, error) {
	if err := b.config.Validate(); err != nil {
		return FactoryConfig{}, err
	}
	return b.config, nil
}

// BuildUnsafe creates the FactoryConfig without validation (use with caution)
func (b *ConfigBuilder) BuildUnsafe() FactoryConfig {
	return b.config
}

// Factory is responsible for creating PubSub instances based on configuration
type Factory struct {
	config FactoryConfig
	logger watermill.LoggerAdapter
}

// NewFactory creates a new PubSub instance based on the provided configuration
// It validates the configuration and returns an appropriate PubSub implementation
func NewFactory(config FactoryConfig) (PubSub, error) {
	// Validate configuration before proceeding
	if err := config.Validate(); err != nil {
		return nil, err
	}

	f := &Factory{
		config: config,
		logger: watermill.NewStdLogger(config.Debug, config.Trace),
	}

	// Create the appropriate PubSub implementation based on type
	switch TypePubsub(strings.ToLower(f.config.PubsubType)) {
	case Kafka:
		return f.createKafka()
	case NATS:
		return f.createNATS()
	case RabbitMQ:
		return f.createRabbitMQ()
	case Redis:
		return f.createRedis()
	default:
		// This should not happen due to validation, but keeping as safety net
		return nil, ErrUnsupportedPubSubType
	}
}
