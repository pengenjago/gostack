package pubsub

import (
	"context"

	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

type rabbitPubSub struct {
	publisher  *amqp.Publisher
	subscriber *amqp.Subscriber
}

func (f *Factory) createRabbitMQ() (PubSub, error) {
	amqpConfig := amqp.NewDurableQueueConfig(f.pubsubUrl)

	publisher, err := amqp.NewPublisher(amqpConfig, f.logger)
	if err != nil {
		return nil, err
	}

	subscriber, err := amqp.NewSubscriber(amqpConfig, f.logger)
	if err != nil {
		return nil, err
	}

	return &rabbitPubSub{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (r *rabbitPubSub) Publish(topic string, messages ...*message.Message) error {
	return r.publisher.Publish(topic, messages...)
}

func (r *rabbitPubSub) Subscribe(topic string) (<-chan *message.Message, error) {
	return r.subscriber.Subscribe(context.Background(), topic)
}

func (r *rabbitPubSub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
