package pubsub

import (
	"context"
	"fmt"
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
	quesubscriber *nats.Subscriber
}

func (f *Factory) createNATS() (PubSub, error) {
	marshaler := &nats.GobMarshaler{}
	logger := watermill.NewStdLogger(f.config.Debug, f.config.Trace)

	options := []nc.Option{
		nc.RetryOnFailedConnect(true),
		nc.Timeout(30 * time.Second),
		nc.ReconnectWait(1 * time.Second),
	}

	jsConfig := nats.JetStreamConfig{Disabled: true}

	subsConfig := nats.SubscriberConfig{
		URL:              f.config.PubsubUrl,
		CloseTimeout:     30 * time.Second,
		AckWaitTimeout:   30 * time.Second,
		NatsOptions:      options,
		Unmarshaler:      marshaler,
		JetStream:        jsConfig,
	}

	subscriber, err := nats.NewSubscriber(
		subsConfig,
		logger,
	)
	if err != nil {
		return nil, err
	}

	subsConfig.QueueGroupPrefix = f.config.Group
	quesubscriber, err := nats.NewSubscriber(
		subsConfig,
		logger,
	)
	if err != nil {
		return nil, err
	}

	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL:         f.config.PubsubUrl,
			NatsOptions: options,
			Marshaler:   marshaler,
			JetStream:   jsConfig,
		},
		logger,
	)

	return &natsPubSub{
		publisher:  publisher,
		subscriber: subscriber,
		quesubscriber: quesubscriber,
	}, nil
}

func (n *natsPubSub) Publish(topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}

	return n.publisher.Publish(topic, &messages)
}

func (n *natsPubSub) Subscribe(topic string, eventHandler PubsubEventHandler) {
	messages, err := n.subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		log.Println(fmt.Sprintf("error subscribe topic %s with error : %v", topic, err.Error()))
		return
	}

	for msg := range messages {
		eventHandler(string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}

func (n *natsPubSub) QueueSubscribe(topic string, eventHandler PubsubEventHandler) {
	messages, err := n.quesubscriber.Subscribe(context.Background(), topic)
	if err != nil {
		log.Println(fmt.Sprintf("error subscribe topic %s with error : %v", topic, err.Error()))
		return
	}

	for msg := range messages {
		eventHandler(string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}

func (n *natsPubSub) Close() error {
	if err := n.publisher.Close(); err != nil {
		return err
	}
	return n.subscriber.Close()
}
