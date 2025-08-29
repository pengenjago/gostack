package pubsub

import "errors"

var (
	// ErrUnsupportedPubSubType is returned when an unsupported pubsub type is provided
	ErrUnsupportedPubSubType = errors.New("unsupported pubsub type")
	// ErrInvalidConfig is returned when the configuration is invalid
	ErrInvalidConfig = errors.New("invalid configuration")
	// ErrMissingPubSubType is returned when pubsub type is not specified
	ErrMissingPubSubType = errors.New("pubsub type is required")
	// ErrMissingPubSubURL is returned when pubsub URL is not specified
	ErrMissingPubSubURL = errors.New("pubsub URL is required")
)