package s3

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// mockWriterAt is a mock implementation of io.WriterAt for testing
type mockWriterAt struct {
	buffer *bytes.Buffer
}

func newMockWriterAt() *mockWriterAt {
	return &mockWriterAt{
		buffer: &bytes.Buffer{},
	}
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	// For simplicity, just append to buffer (ignoring offset)
	return m.buffer.Write(p)
}

func (m *mockWriterAt) Bytes() []byte {
	return m.buffer.Bytes()
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Timeout:         30 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "missing region",
			config: Config{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
			wantErr: true,
		},
		{
			name: "missing access key",
			config: Config{
				Region:          "us-east-1",
				SecretAccessKey: "test-secret-key",
			},
			wantErr: true,
		},
		{
			name: "missing secret key",
			config: Config{
				Region:      "us-east-1",
				AccessKeyID: "test-access-key",
			},
			wantErr: true,
		},
		{
			name: "zero timeout gets default",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Timeout:         0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Check that default timeout is set when zero
			if !tt.wantErr && tt.config.Timeout == 0 {
				t.Errorf("Expected default timeout to be set, got %v", tt.config.Timeout)
			}
		})
	}
}

func TestConfigBuilder(t *testing.T) {
	builder := NewConfigBuilder()

	// Test fluent interface
	config, err := builder.
		WithRegion("us-west-2").
		WithCredentials("test-key", "test-secret").
		WithSessionToken("test-token").
		WithEndpoint("https://s3.example.com").
		WithPathStyle(true).
		WithTimeout(60 * time.Second).
		Build()

	if err != nil {
		t.Fatalf("ConfigBuilder.Build() error = %v", err)
	}

	// Verify all fields are set correctly
	if config.Region != "us-west-2" {
		t.Errorf("Expected Region = us-west-2, got %s", config.Region)
	}
	if config.AccessKeyID != "test-key" {
		t.Errorf("Expected AccessKeyID = test-key, got %s", config.AccessKeyID)
	}
	if config.SecretAccessKey != "test-secret" {
		t.Errorf("Expected SecretAccessKey = test-secret, got %s", config.SecretAccessKey)
	}
	if config.SessionToken != "test-token" {
		t.Errorf("Expected SessionToken = test-token, got %s", config.SessionToken)
	}
	if config.Endpoint != "https://s3.example.com" {
		t.Errorf("Expected Endpoint = https://s3.example.com, got %s", config.Endpoint)
	}
	if !config.UsePathStyle {
		t.Errorf("Expected UsePathStyle = true, got %v", config.UsePathStyle)
	}
	if config.Timeout != 60*time.Second {
		t.Errorf("Expected Timeout = 60s, got %v", config.Timeout)
	}
}

func TestConfigBuilder_BuildWithInvalidConfig(t *testing.T) {
	builder := NewConfigBuilder()

	// Build without required fields should fail
	_, err := builder.Build()
	if err == nil {
		t.Error("Expected Build() to fail with invalid config")
	}

	// BuildUnsafe should not validate
	config := builder.BuildUnsafe()
	if config.Region != "" {
		t.Error("BuildUnsafe should return config without validation")
	}
}

func TestValidateUploadRequest(t *testing.T) {
	client := &client{}

	tests := []struct {
		name    string
		req     *UploadRequest
		wantErr bool
	}{
		{
			name: "valid request",
			req: &UploadRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
				Body:   strings.NewReader("test data"),
			},
			wantErr: false,
		},
		{
			name:    "nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "missing bucket",
			req: &UploadRequest{
				Key:  "test-key",
				Body: strings.NewReader("test data"),
			},
			wantErr: true,
		},
		{
			name: "missing key",
			req: &UploadRequest{
				Bucket: "test-bucket",
				Body:   strings.NewReader("test data"),
			},
			wantErr: true,
		},
		{
			name: "missing body",
			req: &UploadRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.validateUploadRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateUploadRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDownloadRequest(t *testing.T) {
	client := &client{}

	tests := []struct {
		name    string
		req     *DownloadRequest
		wantErr bool
	}{
		{
			name: "valid request",
			req: &DownloadRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
				Writer: newMockWriterAt(),
			},
			wantErr: false,
		},
		{
			name:    "nil request",
			req:     nil,
			wantErr: true,
		},
		{
			name: "missing bucket",
			req: &DownloadRequest{
				Key:    "test-key",
				Writer: newMockWriterAt(),
			},
			wantErr: true,
		},
		{
			name: "missing key",
			req: &DownloadRequest{
				Bucket: "test-bucket",
				Writer: newMockWriterAt(),
			},
			wantErr: true,
		},
		{
			name: "missing writer",
			req: &DownloadRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.validateDownloadRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDownloadRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProgressReader(t *testing.T) {
	data := "Hello, World!"
	reader := strings.NewReader(data)

	var transferred, total int64
	callback := func(t, tot int64) {
		transferred = t
		total = tot
	}

	pr := &progressReader{
		reader:   reader,
		callback: callback,
		total:    int64(len(data)),
	}

	buf := make([]byte, 5)
	n, err := pr.Read(buf)
	if err != nil {
		t.Fatalf("progressReader.Read() error = %v", err)
	}

	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}

	if transferred != 5 {
		t.Errorf("Expected transferred = 5, got %d", transferred)
	}

	if total != int64(len(data)) {
		t.Errorf("Expected total = %d, got %d", len(data), total)
	}
}

func TestProgressWriterAt(t *testing.T) {
	mockWriter := newMockWriterAt()

	var transferred, total int64
	callback := func(t, tot int64) {
		transferred = t
		total = tot
	}

	pw := &progressWriterAt{
		writer:   mockWriter,
		callback: callback,
		total:    100,
	}

	data := []byte("Hello")
	n, err := pw.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("progressWriterAt.WriteAt() error = %v", err)
	}

	if n != len(data) {
		t.Errorf("Expected to write %d bytes, got %d", len(data), n)
	}

	if transferred != int64(len(data)) {
		t.Errorf("Expected transferred = %d, got %d", len(data), transferred)
	}

	if total != 100 {
		t.Errorf("Expected total = 100, got %d", total)
	}
}

func TestS3Error(t *testing.T) {
	// Test basic error
	err := newS3Error("test error")
	if err.Error() != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", err.Error())
	}

	// Test wrapped error
	originalErr := newS3Error("original error")
	wrappedErr := err.Wrap(originalErr)
	expected := "test error: original error"
	if wrappedErr.Error() != expected {
		t.Errorf("Expected wrapped error '%s', got '%s'", expected, wrappedErr.Error())
	}

	// Test unwrap
	if unwrapped := wrappedErr.(*S3Error).Unwrap(); unwrapped != originalErr {
		t.Errorf("Expected unwrapped error to be original error")
	}
}

// Example test showing how to use the S3 client (would require actual AWS credentials)
func ExampleNewClient() {
	// Using builder pattern
	config, err := NewConfigBuilder().
		WithRegion("us-east-1").
		WithCredentials("your-access-key", "your-secret-key").
		WithTimeout(60 * time.Second).
		Build()
	if err != nil {
		panic(err)
	}

	// Create client
	client, err := NewClient(config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Upload example
	uploadReq := &UploadRequest{
		Bucket:      "my-bucket",
		Key:         "my-file.txt",
		Body:        strings.NewReader("Hello, S3!"),
		ContentType: "text/plain",
		ACL:         types.ObjectCannedACLPublicRead,
	}

	ctx := context.Background()
	uploadResult, err := client.Upload(ctx, uploadReq)
	if err != nil {
		panic(err)
	}

	_ = uploadResult // Use the result

	// Download example
	writer := newMockWriterAt()
	downloadReq := &DownloadRequest{
		Bucket: "my-bucket",
		Key:    "my-file.txt",
		Writer: writer,
	}

	downloadResult, err := client.Download(ctx, downloadReq)
	if err != nil {
		panic(err)
	}

	_ = downloadResult // Use the result
}