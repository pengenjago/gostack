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
	factory       *Factory
	publisher     *amqp.Publisher
	subscriber    *amqp.Subscriber
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
		factory:       f,
		publisher:     publisher,
		subscriber:    subscriber,
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

func (r *rabbitPubSub) QueueSubscribe(topic, group string, eventHandler PubsubEventHandler) {
	if group == "" {
		log.Println("Customer Group cannot be empty")
		return
	}

	quesubscriber, _ := amqp.NewSubscriber(amqp.NewDurablePubSubConfig(r.factory.config.PubsubUrl, amqp.GenerateQueueNameConstant(group)), r.factory.logger)

	messages, err := quesubscriber.Subscribe(context.Background(), topic)
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
