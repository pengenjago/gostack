# S3 Upload/Download Library

A comprehensive Go library for Amazon S3 and S3-compatible storage services, providing simple and efficient upload/download operations with progress tracking, context support, and robust error handling.

## Features

- **Simple Interface**: Clean and intuitive API for S3 operations
- **Progress Tracking**: Built-in progress callbacks for upload/download operations
- **Context Support**: Full context.Context support for timeouts and cancellation
- **Builder Pattern**: Fluent configuration builder for easy setup
- **Error Handling**: Comprehensive error types with proper error wrapping
- **S3 Compatible**: Works with Amazon S3 and S3-compatible services (MinIO, DigitalOcean Spaces, etc.)
- **Validation**: Built-in request and configuration validation
- **Comprehensive Testing**: Full test coverage with examples

## Installation

```bash
go get github.com/your-org/gostack/s3
```

## Quick Start

### Basic Upload and Download

```go
package main

import (
    "context"
    "fmt"
    "log"
    "strings"
    "time"
    
    "github.com/your-org/gostack/s3"
)

func main() {
    // Create S3 client using builder pattern
    config, err := s3.NewConfigBuilder().
        WithRegion("us-east-1").
        WithCredentials("your-access-key-id", "your-secret-access-key").
        WithTimeout(60 * time.Second).
        Build()
    if err != nil {
        log.Fatal(err)
    }

    client, err := s3.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Upload a file
    uploadReq := &s3.UploadRequest{
        Bucket:      "my-bucket",
        Key:         "documents/example.txt",
        Body:        strings.NewReader("Hello, S3!"),
        ContentType: "text/plain",
    }

    result, err := client.Upload(ctx, uploadReq)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Upload successful: %s\n", result.Location)
}
```

### Upload with Progress Tracking

```go
progressCallback := func(transferred, total int64) {
    if total > 0 {
        percentage := float64(transferred) / float64(total) * 100
        fmt.Printf("\rProgress: %.2f%% (%d/%d bytes)", percentage, transferred, total)
    }
}

result, err := client.UploadWithProgress(ctx, uploadReq, progressCallback)
```

### Download with Progress Tracking

```go
file, err := os.Create("downloaded-file.txt")
if err != nil {
    log.Fatal(err)
}
defer file.Close()

downloadReq := &s3.DownloadRequest{
    Bucket: "my-bucket",
    Key:    "documents/example.txt",
    Writer: file,
}

progressCallback := func(transferred, total int64) {
    percentage := float64(transferred) / float64(total) * 100
    fmt.Printf("\rDownload: %.2f%%", percentage)
}

result, err := client.DownloadWithProgress(ctx, downloadReq, progressCallback)
```

## Configuration

### Using Builder Pattern (Recommended)

```go
config, err := s3.NewConfigBuilder().
    WithRegion("us-west-2").
    WithCredentials("access-key", "secret-key").
    WithSessionToken("session-token").        // Optional, for temporary credentials
    WithEndpoint("https://s3.example.com").  // Optional, for S3-compatible services
    WithPathStyle(true).                      // Optional, for path-style addressing
    WithTimeout(30 * time.Second).           // Optional, default is 30s
    Build()
```

### Direct Configuration

```go
config := s3.Config{
    Region:          "us-east-1",
    AccessKeyID:     "your-access-key-id",
    SecretAccessKey: "your-secret-access-key",
    Timeout:         60 * time.Second,
}

if err := config.Validate(); err != nil {
    log.Fatal(err)
}
```

## S3-Compatible Services

### MinIO

```go
config, err := s3.NewConfigBuilder().
    WithRegion("us-east-1").
    WithCredentials("minioadmin", "minioadmin").
    WithEndpoint("http://localhost:9000").
    WithPathStyle(true).
    Build()
```

### DigitalOcean Spaces

```go
config, err := s3.NewConfigBuilder().
    WithRegion("nyc3").
    WithCredentials("your-spaces-key", "your-spaces-secret").
    WithEndpoint("https://nyc3.digitaloceanspaces.com").
    Build()
```

## Advanced Usage

### Upload with Metadata and ACL

```go
uploadReq := &s3.UploadRequest{
    Bucket:      "my-bucket",
    Key:         "documents/example.txt",
    Body:        file,
    ContentType: "application/pdf",
    Metadata: map[string]string{
        "author":      "john-doe",
        "department":  "engineering",
        "created-by":  "my-app",
    },
    ACL:          types.ObjectCannedACLPublicRead,
    StorageClass: types.StorageClassStandardIa,
}
```

### Download with Version ID and Range

```go
downloadReq := &s3.DownloadRequest{
    Bucket:    "my-bucket",
    Key:       "documents/example.txt",
    Writer:    file,
    VersionID: "version-id-here",
    Range:     "bytes=0-1023", // Download first 1024 bytes
}
```

### Context with Timeout

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

result, err := client.Upload(ctx, uploadReq)
if err != nil {
    if ctx.Err() == context.DeadlineExceeded {
        fmt.Println("Operation timed out")
    } else {
        fmt.Printf("Operation failed: %v\n", err)
    }
}
```

## Error Handling

The library provides specific error types for different failure scenarios:

```go
_, err := client.Upload(ctx, uploadReq)
if err != nil {
    switch {
    case err == s3.ErrMissingBucket:
        fmt.Println("Bucket name is required")
    case err == s3.ErrMissingKey:
        fmt.Println("Object key is required")
    case err == s3.ErrMissingBody:
        fmt.Println("Request body is required")
    default:
        // Check if it's a wrapped error
        var s3Err *s3.S3Error
        if errors.As(err, &s3Err) {
            fmt.Printf("S3 operation failed: %v\n", s3Err)
            if s3Err.Unwrap() != nil {
                fmt.Printf("Underlying error: %v\n", s3Err.Unwrap())
            }
        } else {
            fmt.Printf("Unexpected error: %v\n", err)
        }
    }
}
```

## API Reference

### Interfaces

#### S3Client
```go
type S3Client interface {
    Uploader
    Downloader
    Close() error
}
```

#### Uploader
```go
type Uploader interface {
    Upload(ctx context.Context, req *UploadRequest) (*UploadResult, error)
    UploadWithProgress(ctx context.Context, req *UploadRequest, callback ProgressCallback) (*UploadResult, error)
}
```

#### Downloader
```go
type Downloader interface {
    Download(ctx context.Context, req *DownloadRequest) (*DownloadResult, error)
    DownloadWithProgress(ctx context.Context, req *DownloadRequest, callback ProgressCallback) (*DownloadResult, error)
}
```

### Types

#### UploadRequest
```go
type UploadRequest struct {
    Bucket       string                    // Required: S3 bucket name
    Key          string                    // Required: S3 object key
    Body         io.Reader                 // Required: Data to upload
    ContentType  string                    // Optional: MIME type
    Metadata     map[string]string         // Optional: Object metadata
    ACL          types.ObjectCannedACL     // Optional: Access control list
    StorageClass types.StorageClass        // Optional: Storage class
}
```

#### DownloadRequest
```go
type DownloadRequest struct {
    Bucket    string        // Required: S3 bucket name
    Key       string        // Required: S3 object key
    Writer    io.WriterAt   // Required: Where to write downloaded data
    VersionID string        // Optional: Object version ID
    Range     string        // Optional: Byte range ("bytes=0-1023")
}
```

#### ProgressCallback
```go
type ProgressCallback func(transferred, total int64)
```

## Testing

Run the test suite:

```bash
go test ./...
```

Run tests with coverage:

```bash
go test -cover ./...
```

## Examples

See the `example_usage.go` file for comprehensive examples including:

- Basic upload/download operations
- Progress tracking
- S3-compatible service usage
- Error handling patterns
- Context usage

## Requirements

- Go 1.19 or later
- AWS SDK for Go v2

## Dependencies

```go
require (
    github.com/aws/aws-sdk-go-v2 v1.21.0
    github.com/aws/aws-sdk-go-v2/config v1.18.45
    github.com/aws/aws-sdk-go-v2/credentials v1.13.43
    github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.87
    github.com/aws/aws-sdk-go-v2/service/s3 v1.40.2
)
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Changelog

### v1.0.0
- Initial release
- Basic upload/download functionality
- Progress tracking support
- Context support
- Builder pattern for configuration
- Comprehensive error handling
- S3-compatible service support