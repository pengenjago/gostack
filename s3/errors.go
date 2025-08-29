package s3

import "fmt"

// S3Error represents a custom error type for S3 operations
type S3Error struct {
	message string
	cause   error
}

// Error implements the error interface
func (e *S3Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %v", e.message, e.cause)
	}
	return e.message
}

// Unwrap returns the underlying error
func (e *S3Error) Unwrap() error {
	return e.cause
}

// Wrap wraps an error with additional context
func (e *S3Error) Wrap(err error) error {
	return &S3Error{
		message: e.message,
		cause:   err,
	}
}

// newS3Error creates a new S3Error
func newS3Error(message string) *S3Error {
	return &S3Error{message: message}
}

// Predefined errors for common S3 operations
var (
	// Configuration errors
	ErrMissingRegion          = newS3Error("region is required")
	ErrMissingAccessKeyID     = newS3Error("access key ID is required")
	ErrMissingSecretAccessKey = newS3Error("secret access key is required")
	ErrFailedToCreateAWSConfig = newS3Error("failed to create AWS config")

	// Request validation errors
	ErrInvalidRequest = newS3Error("invalid request")
	ErrMissingBucket  = newS3Error("bucket name is required")
	ErrMissingKey     = newS3Error("object key is required")
	ErrMissingBody    = newS3Error("request body is required")
	ErrMissingWriter  = newS3Error("writer is required")

	// Operation errors
	ErrUploadFailed   = newS3Error("upload operation failed")
	ErrDownloadFailed = newS3Error("download operation failed")
)