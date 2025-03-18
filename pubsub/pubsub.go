package pubsub

type PubSub interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Publish(topic string, msg []byte) error
	Close() error
}

type Subscriber interface {
	Subscribe(topic string) ([]byte, error)
	Close() error
}

type Factory struct {
	// PubsubType is pubsub type such as kafka, nats, rabbitmq
	PubsubType string
	// PubsubUrl is pubsub url such as kafka://localhost:9092, nats://localhost:4222, amqp://localhost:5672
	PubsubUrl  string
	// Debug is log for debug, default is false.
	Debug      bool
	// Trace is log for trace, default is false.
	Trace      bool
}

func NewFactory(config Factory) (PubSub, error) {
	f := &Factory{
		PubsubType: config.PubsubType,
		PubsubUrl:  config.PubsubUrl,
		Debug:      config.Debug,
		Trace:      config.Trace,
	}

	switch TypePubsub(f.PubsubType) {
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