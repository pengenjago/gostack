package pubsub

type TypePubsub string

const (
	Kafka    TypePubsub = "kafka"
	NATS     TypePubsub = "nats"
	RabbitMQ TypePubsub = "rabbitmq"
)
