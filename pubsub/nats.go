package pubsub

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/stan.go"
	"log"
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

func (n *natsPubSub) Publish(topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}

	return n.publisher.Publish(topic, &messages)
}

func (n *natsPubSub) Subscribe(topic string) ([]byte, error) {
	messages, err := n.subscriber.Subscribe(context.Background(), topic)
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()

		return msg.Payload, err
	}

	return nil, err
}

func (n *natsPubSub) Close() error {
	if err := n.publisher.Close(); err != nil {
		return err
	}
	return n.subscriber.Close()
}
