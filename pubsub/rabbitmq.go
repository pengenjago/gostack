package pubsub

import (
	"context"
	"fmt"
	"log"

	"github.com/ThreeDotsLabs/watermill"

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

func (r *rabbitPubSub) Subscribe(topic string, eventHandler PubsubEventHandler) {
	messages, err := r.subscriber.Subscribe(context.Background(), topic)
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

func (r *rabbitPubSub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
