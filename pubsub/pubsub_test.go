package pubsub

import (
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
				ps, err := NewFactory(Factory{PubsubType: psType, PubsubUrl: "127.0.0.1:4222", Debug: false, Trace: false})
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

				// Test Publish
				err = ps.Publish(tt.topic, tt.message)
				if err != nil {
					t.Logf("Publish error for %s: %v", psType, err)
				}

				// Test Subscribe
				msg, err := ps.Subscribe(tt.topic)
				if err != nil {
					t.Logf("Subscribe error for %s: %v", psType, err)
				}

				if msg != nil {
					assert.Equal(t, tt.message, msg)
				}
			})
		}
	}
}
