package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

type rabbitPubSub struct {
	factory       *Factory
	publisher     *amqp.Publisher
	subscriber    *amqp.Subscriber
	quesubscriber *amqp.Subscriber
}

func (f *Factory) createRabbitMQ() (PubSub, error) {
	amqpConfig := amqp.NewDurableQueueConfig(f.config.PubsubUrl)

	publisher, err := amqp.NewPublisher(amqpConfig, f.logger)
	if err != nil {
		return nil, err
	}

	subscriber, err := amqp.NewSubscriber(amqpConfig, f.logger)
	if err != nil {
		return nil, err
	}

	return &rabbitPubSub{
		factory:    f,
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

func (r *rabbitPubSub) Publish(ctx context.Context, topic string, msg []byte) error {

	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}
	return r.publisher.Publish(topic, &messages)
}

func (r *rabbitPubSub) Subscribe(ctx context.Context, topic string, eventHandler PubsubEventHandler) error {
	messages, err := r.subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		return fmt.Errorf("error subscribe topic %s with error : %v", topic, err.Error())
	}

	for msg := range messages {
		eventHandler(string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
	return nil
}

func (r *rabbitPubSub) QueueSubscribe(ctx context.Context, topic, group string, eventHandler PubsubEventHandler) error {
	if group == "" {
		return fmt.Errorf("customer group cannot be empty")
	}

	if r.quesubscriber == nil {
		quesubscriber, _ := amqp.NewSubscriber(amqp.NewDurablePubSubConfig(r.factory.config.PubsubUrl, amqp.GenerateQueueNameConstant(group)), r.factory.logger)
		r.quesubscriber = quesubscriber
	}

	messages, err := r.quesubscriber.Subscribe(context.Background(), topic)
	if err != nil {
		return fmt.Errorf("error QueueSubscribe topic %s with error : %v", topic, err.Error())

	}

	for msg := range messages {
		eventHandler(string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
	return nil
}

func (r *rabbitPubSub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
