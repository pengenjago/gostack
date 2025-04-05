package pubsub

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

type kafkaPubSub struct {
	factory       *Factory
	publisher     *kafka.Publisher
	subscriber    *kafka.Subscriber
	quesubscriber *kafka.Subscriber
}

func (f *Factory) createKafka() (PubSub, error) {

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{f.config.PubsubUrl},
			Marshaler: kafka.DefaultMarshaler{},
		},
		f.logger,
	)
	if err != nil {
		return nil, err
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:     []string{f.config.PubsubUrl},
			Unmarshaler: kafka.DefaultMarshaler{},
		},
		f.logger,
	)
	if err != nil {
		return nil, err
	}

	return &kafkaPubSub{
		factory:    f,
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

func (k *kafkaPubSub) Subscribe(topic string, eventHandler PubsubEventHandler) {
	messages, err := k.subscriber.Subscribe(context.Background(), topic)
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

func (k *kafkaPubSub) QueueSubscribe(topic, group string, eventHandler PubsubEventHandler) {
	if group == "" {
		log.Println("Customer Group cannot be empty")
		return
	}

	if k.quesubscriber == nil {
		saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
		saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

		quesubscriber, _ := kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               []string{k.factory.config.PubsubUrl},
				Unmarshaler:           kafka.DefaultMarshaler{},
				OverwriteSaramaConfig: saramaSubscriberConfig,
				ConsumerGroup:         group,
			},
			k.factory.logger,
		)

		k.quesubscriber = quesubscriber
	}

	messages, err := k.quesubscriber.Subscribe(context.Background(), topic)
	if err != nil {
		log.Println(fmt.Sprintf("error QueueSubscribe topic %s with error : %v", topic, err.Error()))
		return
	}

	for msg := range messages {
		eventHandler(string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}

func (k *kafkaPubSub) Close() error {
	if err := k.publisher.Close(); err != nil {
		return err
	}
	return k.subscriber.Close()
}
