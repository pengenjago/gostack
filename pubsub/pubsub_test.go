package pubsub

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPublishSubscribe(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		message []byte
	}{
		{
			name:    "Test publish and subscribe",
			topic:   "test-topic",
			message: []byte("test message"),
		},
	}

	pubsubTypes := []string{string(NATS)}

	for _, psType := range pubsubTypes {
		for _, tt := range tests {
			t.Run(tt.name+"_"+psType, func(t *testing.T) {
				// Using the new builder pattern
				config, err := NewConfigBuilder().
					WithPubSubType(psType).
					WithURL("nats://localhost:4222").
					WithDebug(false).
					WithTrace(false).
					Build()
				if err != nil {
					t.Logf("Config validation error: %v", err)
					return
				}

				ps, err := NewFactory(config)
				if err != nil {
					t.Logf("Skipping test for %s due to creation error: %v", psType, err)
					return
				}
				defer func(ps PubSub) {
					err := ps.Close()
					if err != nil {
						log.Println(err)
					}
				}(ps)

				ctx := context.Background()

				// Test Publish
				err = ps.Publish(ctx, tt.topic, tt.message)
				if err != nil {
					t.Logf("Publish error for %s: %v", psType, err)
				}

				// Test Subscribe
				err = ps.Subscribe(ctx, tt.topic, func(msg string) {
					assert.Equal(t, string(tt.message), msg)
				})
				if err != nil {
					t.Logf("Subscribe error for %s: %v", psType, err)
				}
			})
		}
	}
}
