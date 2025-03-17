package pubsub

import (
	"github.com/ThreeDotsLabs/watermill"
)

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
