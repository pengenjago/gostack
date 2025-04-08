package pubsub

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"log"
)

type redisPubsub struct {
	factory       *Factory
	subscriber    *redisstream.Subscriber
	publisher     *redisstream.Publisher
	quesubscriber *redisstream.Subscriber
	config        *redisstream.SubscriberConfig
}

func (r *redisPubsub) createRedis() (PubSub, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: r.factory.config.PubsubUrl,
		DB:   0,
	})

	subsConfig := redisstream.SubscriberConfig{
		Client:       redisClient,
		Unmarshaller: redisstream.DefaultMarshallerUnmarshaller{},
	}

	subscriber, err := redisstream.NewSubscriber(subsConfig, r.factory.logger)
	if err != nil {
		return nil, err
	}

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client:     redisClient,
		Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
	}, r.factory.logger)
	if err != nil {
		return nil, err
	}

	return &redisPubsub{
		factory:    r.factory,
		subscriber: subscriber,
		publisher:  publisher,
		config:     &subsConfig,
	}, nil
}

func (r *redisPubsub) Publish(topic string, msg []byte) error {
	messages := message.Message{
		UUID:    watermill.NewUUID(),
		Payload: msg,
	}

	return r.publisher.Publish(topic, &messages)
}

func (r *redisPubsub) Subscribe(topic string, eventHandler PubsubEventHandler) {
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

func (r *redisPubsub) QueueSubscribe(topic, group string, eventHandler PubsubEventHandler) {
	if group == "" {
		log.Println("Customer Group cannot be empty")
		return
	}

	if r.quesubscriber == nil {
		r.config.ConsumerGroup = group
		quesubscriber, _ := redisstream.NewSubscriber(*r.config, r.factory.logger)

		r.quesubscriber = quesubscriber
	}

	messages, err := r.quesubscriber.Subscribe(context.Background(), topic)
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

func (r *redisPubsub) Close() error {
	if err := r.publisher.Close(); err != nil {
		return err
	}
	return r.subscriber.Close()
}
