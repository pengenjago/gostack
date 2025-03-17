package pubsub

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type PubSub interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Publish(topic string, messages ...*message.Message) error
	Close() error
}

type Subscriber interface {
	Subscribe(topic string) (<-chan *message.Message, error)
	Close() error
}

type Factory struct {
	pubsubType string
	pubsubUrl  string
	logger     watermill.LoggerAdapter
}

func NewFactory(pubsubType, pubsubUrl string, logger watermill.LoggerAdapter) *Factory {
	return &Factory{
		pubsubType: pubsubType,
		pubsubUrl:  pubsubUrl,
		logger:     logger,
	}
}

func (f *Factory) Create() (PubSub, error) {
	switch TypePubsub(f.pubsubType) {
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
