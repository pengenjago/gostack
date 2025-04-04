package pubsub

type PubsubEventHandler func(msg string)
type PubSub interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Publish(topic string, msg []byte) error
	Close() error
}

type Subscriber interface {
	Subscribe(topic string, eventHandler PubsubEventHandler)
	QueueSubscribe(topic string, eventHandler PubsubEventHandler)
	Close() error
}

type FactoryConfig struct {
	// PubsubType is pubsub type such as Kafka, NATS, RabbitMq
	PubsubType string
	// PubsubUrl is pubsub url such as kafka://localhost:9092, nats://localhost:4222, amqp://localhost:5672
	PubsubUrl  string
	// Debug is log for debug, default is false.
	Debug      bool
	// Trace is log for trace, default is false.
	Trace      bool
	// Group is for handle Queue Groups (NATS) / Customer Group (Kafka)
	Group string
}

type Factory struct {
	config FactoryConfig
}

func NewFactory(config FactoryConfig) (PubSub, error) {
	f := &Factory{
		config: config,
	}

	switch TypePubsub(f.config.PubsubType) {
	case Kafka:
		return f.createKafka()
	case NATS:
		return f.createNATS()
	case RabbitMQ:
		return f.createRabbitMQ()
	default:
		return nil, ErrUnsupportedPubSubType
	}
}