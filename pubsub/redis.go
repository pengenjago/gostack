package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

type redisPubsub struct {
	factory       *Factory
	subscriber    *redisstream.Subscriber
	publisher     *redisstream.Publisher
	quesubscriber *redisstream.Subscriber
	config        *redisstream.SubscriberConfig
}

func (f *Factory) createRedis() (PubSub, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: f.config.PubsubUrl,
		DB:   0,
	})

	subsConfig := redisstream.SubscriberConfig{
		Client:       redisClient,
		Unmarshaller: redisstream.DefaultMarshallerUnmarshaller{},
	}

	subscriber, err := redisstream.NewSubscriber(subsConfig, f.logger)
	if err != nil {
		return nil, err
	}

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client:     redisClient,
		Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
	}, f.logger)
	if err != nil {
		return nil, err
	}

	return &redisPubsub{
		factory:    f,
		subscriber: subscriber,
		publisher:  publisher,
		config:     &subsConfig,
	}, nil
}

func (r *redisPubsub) Publish(ctx context.Context, topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}

	return r.publisher.Publish(topic, &messages)
}

func (r *redisPubsub) Subscribe(ctx context.Context, topic string, eventHandler PubsubEventHandler) error {
	messages, err := r.subscriber.Subscribe(ctx, topic)
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

func (r *redisPubsub) QueueSubscribe(ctx context.Context, topic, group string, eventHandler PubsubEventHandler) error {
	if group == "" {
		return fmt.Errorf("consumer group cannot be empty")
	}

	if r.quesubscriber == nil {
		r.config.ConsumerGroup = group
		quesubscriber, _ := redisstream.NewSubscriber(*r.config, r.factory.logger)

		r.quesubscriber = quesubscriber
	}

	messages, err := r.quesubscriber.Subscribe(context.Background(), topic)
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

func (r *redisPubsub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
