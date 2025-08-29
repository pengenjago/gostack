package s3

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ExampleBasicUploadDownload demonstrates basic upload and download operations
func ExampleBasicUploadDownload() {
	// Create S3 client using builder pattern
	config, err := NewConfigBuilder().
		WithRegion("us-east-1").
		WithCredentials("your-access-key-id", "your-secret-access-key").
		WithTimeout(60 * time.Second).
		Build()
	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Upload a file
	uploadReq := &UploadRequest{
		Bucket:      "my-test-bucket",
		Key:         "documents/example.txt",
		Body:        strings.NewReader("Hello, S3! This is a test file."),
		ContentType: "text/plain",
		Metadata: map[string]string{
			"author":      "example-user",
			"created-by": "s3-library",
		},
		ACL: types.ObjectCannedACLPrivate,
	}

	uploadResult, err := client.Upload(ctx, uploadReq)
	if err != nil {
		log.Fatalf("Upload failed: %v", err)
	}

	fmt.Printf("Upload successful!\n")
	fmt.Printf("Location: %s\n", uploadResult.Location)
	fmt.Printf("ETag: %s\n", uploadResult.ETag)

	// Download the file
	var buf bytes.Buffer
	downloadReq := &DownloadRequest{
		Bucket: "my-test-bucket",
		Key:    "documents/example.txt",
		Writer: &bytesWriterAt{buffer: &buf},
	}

	downloadResult, err := client.Download(ctx, downloadReq)
	if err != nil {
		log.Fatalf("Download failed: %v", err)
	}

	fmt.Printf("Download successful!\n")
	fmt.Printf("Size: %d bytes\n", downloadResult.Size)
	fmt.Printf("Content: %s\n", buf.String())
	fmt.Printf("Last Modified: %s\n", downloadResult.LastModified)
}

// ExampleUploadWithProgress demonstrates upload with progress tracking
func ExampleUploadWithProgress() {
	config, err := NewConfigBuilder().
		WithRegion("us-west-2").
		WithCredentials("your-access-key-id", "your-secret-access-key").
		Build()
	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	defer client.Close()

	// Create a larger file for demonstration
	data := strings.Repeat("This is a test line for upload progress tracking.\n", 1000)

	uploadReq := &UploadRequest{
		Bucket:       "my-test-bucket",
		Key:          "large-files/progress-test.txt",
		Body:         strings.NewReader(data),
		ContentType:  "text/plain",
		StorageClass: types.StorageClassStandard,
	}

	// Progress callback
	progressCallback := func(transferred, total int64) {
		if total > 0 {
			percentage := float64(transferred) / float64(total) * 100
			fmt.Printf("\rUpload progress: %.2f%% (%d/%d bytes)", percentage, transferred, total)
		} else {
			fmt.Printf("\rUploaded: %d bytes", transferred)
		}
	}

	ctx := context.Background()
	uploadResult, err := client.UploadWithProgress(ctx, uploadReq, progressCallback)
	if err != nil {
		log.Fatalf("\nUpload failed: %v", err)
	}

	fmt.Printf("\nUpload completed successfully!\n")
	fmt.Printf("Location: %s\n", uploadResult.Location)
}

// ExampleDownloadWithProgress demonstrates download with progress tracking
func ExampleDownloadWithProgress() {
	config, err := NewConfigBuilder().
		WithRegion("us-west-2").
		WithCredentials("your-access-key-id", "your-secret-access-key").
		Build()
	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	defer client.Close()

	// Create a file to download to
	file, err := os.Create("downloaded-file.txt")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	downloadReq := &DownloadRequest{
		Bucket: "my-test-bucket",
		Key:    "large-files/progress-test.txt",
		Writer: file,
	}

	// Progress callback
	progressCallback := func(transferred, total int64) {
		if total > 0 {
			percentage := float64(transferred) / float64(total) * 100
			fmt.Printf("\rDownload progress: %.2f%% (%d/%d bytes)", percentage, transferred, total)
		} else {
			fmt.Printf("\rDownloaded: %d bytes", transferred)
		}
	}

	ctx := context.Background()
	downloadResult, err := client.DownloadWithProgress(ctx, downloadReq, progressCallback)
	if err != nil {
		log.Fatalf("\nDownload failed: %v", err)
	}

	fmt.Printf("\nDownload completed successfully!\n")
	fmt.Printf("Downloaded %d bytes\n", downloadResult.Size)
	fmt.Printf("Content Type: %s\n", downloadResult.ContentType)
}

// ExampleS3CompatibleService demonstrates using the library with S3-compatible services
func ExampleS3CompatibleService() {
	// Example for MinIO or other S3-compatible services
	config, err := NewConfigBuilder().
		WithRegion("us-east-1"). // MinIO uses us-east-1 by default
		WithCredentials("minioadmin", "minioadmin").
		WithEndpoint("http://localhost:9000"). // MinIO default endpoint
		WithPathStyle(true).                    // MinIO requires path-style
		WithTimeout(30 * time.Second).
		Build()
	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Upload to MinIO
	uploadReq := &UploadRequest{
		Bucket:      "test-bucket",
		Key:         "minio-test.txt",
		Body:        strings.NewReader("Hello from MinIO!"),
		ContentType: "text/plain",
	}

	uploadResult, err := client.Upload(ctx, uploadReq)
	if err != nil {
		log.Fatalf("Upload to MinIO failed: %v", err)
	}

	fmt.Printf("MinIO upload successful: %s\n", uploadResult.Location)
}

// ExampleContextWithTimeout demonstrates using context for timeout control
func ExampleContextWithTimeout() {
	config, err := NewConfigBuilder().
		WithRegion("us-east-1").
		WithCredentials("your-access-key-id", "your-secret-access-key").
		Build()
	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	defer client.Close()

	// Create context with custom timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uploadReq := &UploadRequest{
		Bucket: "my-test-bucket",
		Key:    "timeout-test.txt",
		Body:   strings.NewReader("Testing timeout control"),
	}

	uploadResult, err := client.Upload(ctx, uploadReq)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Upload timed out")
		} else {
			log.Fatalf("Upload failed: %v", err)
		}
		return
	}

	fmt.Printf("Upload completed within timeout: %s\n", uploadResult.Location)
}

// ExampleErrorHandling demonstrates proper error handling
func ExampleErrorHandling() {
	// Invalid configuration
	config := Config{
		Region: "us-east-1",
		// Missing credentials
	}

	client, err := NewClient(config)
	if err != nil {
		// Handle configuration errors
		switch {
		case err == ErrMissingAccessKeyID:
			fmt.Println("Access Key ID is required")
		case err == ErrMissingSecretAccessKey:
			fmt.Println("Secret Access Key is required")
		default:
			fmt.Printf("Configuration error: %v\n", err)
		}
		return
	}
	defer client.Close()

	// Invalid upload request
	uploadReq := &UploadRequest{
		// Missing required fields
	}

	ctx := context.Background()
	_, err = client.Upload(ctx, uploadReq)
	if err != nil {
		// Handle request validation errors
		switch {
		case err == ErrMissingBucket:
			fmt.Println("Bucket name is required")
		case err == ErrMissingKey:
			fmt.Println("Object key is required")
		case err == ErrMissingBody:
			fmt.Println("Request body is required")
		default:
			fmt.Printf("Upload error: %v\n", err)
		}
	}
}

// bytesWriterAt implements io.WriterAt for bytes.Buffer
type bytesWriterAt struct {
	buffer *bytes.Buffer
}

func (b *bytesWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	// For simplicity, just append to buffer (ignoring offset)
	return b.buffer.Write(p)
}