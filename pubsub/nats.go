package pubsub

import (
	"context"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/stan.go"
)

type natsPubSub struct {
	publisher  *nats.StreamingPublisher
	subscriber *nats.StreamingSubscriber
}

func (f *Factory) createNATS() (PubSub, error) {
	publisher, err := nats.NewStreamingPublisher(nats.StreamingPublisherConfig{
		StanOptions: []stan.Option{
			stan.NatsURL(f.pubsubUrl),
		},
	}, f.logger)
	if err != nil {
		return nil, err
	}

	subscriber, err := nats.NewStreamingSubscriber(nats.StreamingSubscriberConfig{
		StanOptions: []stan.Option{
			stan.NatsURL(f.pubsubUrl),
		},
	}, f.logger)
	if err != nil {
		return nil, err
	}

	return &natsPubSub{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (n *natsPubSub) Publish(topic string, messages ...*message.Message) error {
	return n.publisher.Publish(topic, messages...)
}

func (n *natsPubSub) Subscribe(topic string) (<-chan *message.Message, error) {
	return n.subscriber.Subscribe(context.Background(), topic)
}

func (n *natsPubSub) Close() error {
	if err := n.publisher.Close(); err != nil {
		return err
	}
	return n.subscriber.Close()
}
