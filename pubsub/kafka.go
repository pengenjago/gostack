package pubsub

import (
	"context"
	"log"

	"github.com/ThreeDotsLabs/watermill"

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
			Brokers:   []string{f.PubsubUrl},
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(f.Debug, f.Trace),
	)
	if err != nil {
		return nil, err
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:     []string{f.PubsubUrl},
			Unmarshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(f.Debug, f.Trace),
	)
	if err != nil {
		return nil, err
	}

	return &kafkaPubSub{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (k *kafkaPubSub) Publish(topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}
	return k.publisher.Publish(topic, &messages)
}

func (k *kafkaPubSub) Subscribe(topic string) ([]byte, error) {
	messages, err := k.subscriber.Subscribe(context.Background(), topic)

	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()

		return msg.Payload, err
	}

	return nil, err
}

func (k *kafkaPubSub) Close() error {
	if err := k.publisher.Close(); err != nil {
		return err
	}
	return k.subscriber.Close()
}
