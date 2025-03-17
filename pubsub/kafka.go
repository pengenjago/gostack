package pubsub

import (
	"context"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type kafkaPubSub struct {
	publisher  *kafka.Publisher
	subscriber *kafka.Subscriber
}

func (f *Factory) createKafka() (PubSub, error) {
	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{f.pubsubUrl},
			Marshaler: kafka.DefaultMarshaler{},
		},
		f.logger,
	)
	if err != nil {
		return nil, err
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:     []string{f.pubsubUrl},
			Unmarshaler: kafka.DefaultMarshaler{},
		},
		f.logger,
	)
	if err != nil {
		return nil, err
	}

	return &kafkaPubSub{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (k *kafkaPubSub) Publish(topic string, messages ...*message.Message) error {
	return k.publisher.Publish(topic, messages...)
}

func (k *kafkaPubSub) Subscribe(topic string) (<-chan *message.Message, error) {
	return k.subscriber.Subscribe(context.Background(), topic)
}

func (k *kafkaPubSub) Close() error {
	if err := k.publisher.Close(); err != nil {
		return err
	}
	return k.subscriber.Close()
}
