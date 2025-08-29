package pubsub

import (
	"context"
	"fmt"

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

func (k *kafkaPubSub) Publish(ctx context.Context, topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}
	return k.publisher.Publish(topic, &messages)
}

func (k *kafkaPubSub) Subscribe(ctx context.Context, topic string, eventHandler PubsubEventHandler) error {
	messages, err := k.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				eventHandler(string(msg.Payload))
				// Acknowledge that we received and processed the message
				msg.Ack()
			}
		}
	}()

	return nil
}

func (k *kafkaPubSub) QueueSubscribe(ctx context.Context, topic, group string, eventHandler PubsubEventHandler) error {
	if group == "" {
		return fmt.Errorf("consumer group cannot be empty")
	}

	if k.quesubscriber == nil {
		saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
		saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

		quesubscriber, err := kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               []string{k.factory.config.PubsubUrl},
				Unmarshaler:           kafka.DefaultMarshaler{},
				OverwriteSaramaConfig: saramaSubscriberConfig,
				ConsumerGroup:         group,
			},
			k.factory.logger,
		)
		if err != nil {
			return fmt.Errorf("failed to create queue subscriber: %w", err)
		}

		k.quesubscriber = quesubscriber
	}

	messages, err := k.quesubscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to queue subscribe to topic %s: %w", topic, err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				eventHandler(string(msg.Payload))
				// Acknowledge that we received and processed the message
				msg.Ack()
			}
		}
	}()

	return nil
}

func (k *kafkaPubSub) Close() error {
	var errs []error

	if k.publisher != nil {
		if err := k.publisher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close publisher: %w", err))
		}
	}

	if k.subscriber != nil {
		if err := k.subscriber.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close subscriber: %w", err))
		}
	}

	if k.quesubscriber != nil {
		if err := k.quesubscriber.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close queue subscriber: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka pubsub: %v", errs)
	}

	return nil
}
