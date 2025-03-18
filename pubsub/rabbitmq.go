package pubsub

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"log"

	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

type rabbitPubSub struct {
	publisher  *amqp.Publisher
	subscriber *amqp.Subscriber
}

func (f *Factory) createRabbitMQ() (PubSub, error) {
	amqpConfig := amqp.NewDurableQueueConfig(f.PubsubUrl)

	publisher, err := amqp.NewPublisher(amqpConfig, watermill.NewStdLogger(f.Debug, f.Trace))
	if err != nil {
		return nil, err
	}

	subscriber, err := amqp.NewSubscriber(amqpConfig, watermill.NewStdLogger(f.Debug, f.Trace))
	if err != nil {
		return nil, err
	}

	return &rabbitPubSub{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (r *rabbitPubSub) Publish(topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}
	return r.publisher.Publish(topic, &messages)
}

func (r *rabbitPubSub) Subscribe(topic string) ([]byte, error) {
	messages, err := r.subscriber.Subscribe(context.Background(), topic)

	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()

		return msg.Payload, err
	}

	return nil, err
}

func (r *rabbitPubSub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
