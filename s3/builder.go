package s3

import "time"

// ConfigBuilder provides a fluent interface for building S3 configurations
type ConfigBuilder struct {
	config Config
}

// NewConfigBuilder creates a new ConfigBuilder with default values
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: Config{
			Timeout: 30 * time.Second,
		},
	}
}

// WithRegion sets the AWS region
func (b *ConfigBuilder) WithRegion(region string) *ConfigBuilder {
	b.config.Region = region
	return b
}

// WithCredentials sets the AWS credentials
func (b *ConfigBuilder) WithCredentials(accessKeyID, secretAccessKey string) *ConfigBuilder {
	b.config.AccessKeyID = accessKeyID
	b.config.SecretAccessKey = secretAccessKey
	return b
}

// WithSessionToken sets the AWS session token for temporary credentials
func (b *ConfigBuilder) WithSessionToken(sessionToken string) *ConfigBuilder {
	b.config.SessionToken = sessionToken
	return b
}

// WithEndpoint sets a custom S3 endpoint (for S3-compatible services)
func (b *ConfigBuilder) WithEndpoint(endpoint string) *ConfigBuilder {
	b.config.Endpoint = endpoint
	return b
}

// WithPathStyle enables path-style addressing
func (b *ConfigBuilder) WithPathStyle(usePathStyle bool) *ConfigBuilder {
	b.config.UsePathStyle = usePathStyle
	return b
}

// WithTimeout sets the default timeout for operations
func (b *ConfigBuilder) WithTimeout(timeout time.Duration) *ConfigBuilder {
	b.config.Timeout = timeout
	return b
}

// Build creates and validates the configuration
func (b *ConfigBuilder) Build() (Config, error) {
	if err := b.config.Validate(); err != nil {
		return Config{}, err
	}
	return b.config, nil
}

// BuildUnsafe creates the configuration without validation
// Use this method only when you're certain the configuration is valid
func (b *ConfigBuilder) BuildUnsafe() Config {
	return b.config
}