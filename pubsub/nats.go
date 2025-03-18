package pubsub

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
)

type natsPubSub struct {
	publisher  *nats.Publisher
	subscriber *nats.Subscriber
}

func (f *Factory) createNATS() (PubSub, error) {
	marshaler := &nats.GobMarshaler{}
	logger := watermill.NewStdLogger(f.Debug, f.Trace)

	options := []nc.Option{
		nc.RetryOnFailedConnect(true),
		nc.Timeout(30 * time.Second),
		nc.ReconnectWait(1 * time.Second),
	}

	jsConfig := nats.JetStreamConfig{Disabled: true}
	subscriber, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:            f.PubsubUrl,
			CloseTimeout:   30 * time.Second,
			AckWaitTimeout: 30 * time.Second,
			NatsOptions:    options,
			Unmarshaler:    marshaler,
			JetStream:      jsConfig,
		},
		logger,
	)
	if err != nil {
		return nil, err
	}

	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL:         f.PubsubUrl,
			NatsOptions: options,
			Marshaler:   marshaler,
			JetStream:   jsConfig,
		},
		logger,
	)

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
