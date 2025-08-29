package s3

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ProgressCallback defines the function signature for upload/download progress tracking
type ProgressCallback func(transferred, total int64)

// S3Client defines the interface for S3 operations
type S3Client interface {
	Uploader
	Downloader
	Close() error
}

// Uploader defines the interface for uploading files to S3
type Uploader interface {
	// Upload uploads data to S3 with optional progress tracking
	Upload(ctx context.Context, req *UploadRequest) (*UploadResult, error)
	// UploadWithProgress uploads data to S3 with progress callback
	UploadWithProgress(ctx context.Context, req *UploadRequest, callback ProgressCallback) (*UploadResult, error)
}

// Downloader defines the interface for downloading files from S3
type Downloader interface {
	// Download downloads data from S3
	Download(ctx context.Context, req *DownloadRequest) (*DownloadResult, error)
	// DownloadWithProgress downloads data from S3 with progress callback
	DownloadWithProgress(ctx context.Context, req *DownloadRequest, callback ProgressCallback) (*DownloadResult, error)
}

// Config holds the configuration for S3 client
type Config struct {
	// Region is the AWS region (required)
	Region string
	// AccessKeyID is the AWS access key ID (required)
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key (required)
	SecretAccessKey string
	// SessionToken is the AWS session token (optional, for temporary credentials)
	SessionToken string
	// Endpoint is the custom S3 endpoint (optional, for S3-compatible services)
	Endpoint string
	// UsePathStyle forces path-style addressing (optional, default: false)
	UsePathStyle bool
	// Timeout is the default timeout for operations (optional, default: 30s)
	Timeout time.Duration
}

// Validate checks if the Config is valid and returns an error if not
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Region) == "" {
		return ErrMissingRegion
	}

	if strings.TrimSpace(c.AccessKeyID) == "" {
		return ErrMissingAccessKeyID
	}

	if strings.TrimSpace(c.SecretAccessKey) == "" {
		return ErrMissingSecretAccessKey
	}

	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}

	return nil
}

// UploadRequest represents a request to upload data to S3
type UploadRequest struct {
	// Bucket is the S3 bucket name (required)
	Bucket string
	// Key is the S3 object key (required)
	Key string
	// Body is the data to upload (required)
	Body io.Reader
	// ContentType is the MIME type of the content (optional)
	ContentType string
	// Metadata is additional metadata for the object (optional)
	Metadata map[string]string
	// ACL is the access control list for the object (optional)
	ACL types.ObjectCannedACL
	// StorageClass is the storage class for the object (optional)
	StorageClass types.StorageClass
}

// UploadResult represents the result of an upload operation
type UploadResult struct {
	// Location is the URL of the uploaded object
	Location string
	// ETag is the entity tag of the uploaded object
	ETag string
	// VersionID is the version ID of the uploaded object (if versioning is enabled)
	VersionID string
	// Size is the size of the uploaded object in bytes
	Size int64
}

// DownloadRequest represents a request to download data from S3
type DownloadRequest struct {
	// Bucket is the S3 bucket name (required)
	Bucket string
	// Key is the S3 object key (required)
	Key string
	// Writer is where to write the downloaded data (required)
	Writer io.WriterAt
	// VersionID is the version ID of the object to download (optional)
	VersionID string
	// Range is the byte range to download (optional, format: "bytes=0-1023")
	Range string
}

// DownloadResult represents the result of a download operation
type DownloadResult struct {
	// Size is the size of the downloaded data in bytes
	Size int64
	// ETag is the entity tag of the downloaded object
	ETag string
	// LastModified is the last modified time of the object
	LastModified time.Time
	// ContentType is the MIME type of the downloaded content
	ContentType string
	// Metadata is the metadata of the downloaded object
	Metadata map[string]string
}

// client implements the S3Client interface
type client struct {
	config     Config
	s3Client   *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

// NewClient creates a new S3 client with the provided configuration
func NewClient(cfg Config) (S3Client, error) {
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Create AWS config
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)),
	)
	if err != nil {
		return nil, ErrFailedToCreateAWSConfig.Wrap(err)
	}

	// Create S3 client with custom options
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	// Create uploader and downloader
	uploader := manager.NewUploader(s3Client)
	downloader := manager.NewDownloader(s3Client)

	return &client{
		config:     cfg,
		s3Client:   s3Client,
		uploader:   uploader,
		downloader: downloader,
	}, nil
}

// Upload uploads data to S3
func (c *client) Upload(ctx context.Context, req *UploadRequest) (*UploadResult, error) {
	return c.UploadWithProgress(ctx, req, nil)
}

// UploadWithProgress uploads data to S3 with progress tracking
func (c *client) UploadWithProgress(ctx context.Context, req *UploadRequest, callback ProgressCallback) (*UploadResult, error) {
	if err := c.validateUploadRequest(req); err != nil {
		return nil, err
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// Prepare upload input
	input := &s3.PutObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
		Body:   req.Body,
	}

	if req.ContentType != "" {
		input.ContentType = aws.String(req.ContentType)
	}

	if len(req.Metadata) > 0 {
		input.Metadata = req.Metadata
	}

	if req.ACL != "" {
		input.ACL = req.ACL
	}

	if req.StorageClass != "" {
		input.StorageClass = req.StorageClass
	}

	// Track progress if callback is provided
	var totalSize int64
	if callback != nil {
		// Wrap the body with progress tracking
		req.Body = &progressReader{
			reader:   req.Body,
			callback: callback,
			total:    totalSize,
		}
	}

	// Perform upload
	result, err := c.uploader.Upload(ctx, input)
	if err != nil {
		return nil, ErrUploadFailed.Wrap(err)
	}

	return &UploadResult{
		Location:  result.Location,
		ETag:      aws.ToString(result.ETag),
		VersionID: aws.ToString(result.VersionID),
	}, nil
}

// Download downloads data from S3
func (c *client) Download(ctx context.Context, req *DownloadRequest) (*DownloadResult, error) {
	return c.DownloadWithProgress(ctx, req, nil)
}

// DownloadWithProgress downloads data from S3 with progress tracking
func (c *client) DownloadWithProgress(ctx context.Context, req *DownloadRequest, callback ProgressCallback) (*DownloadResult, error) {
	if err := c.validateDownloadRequest(req); err != nil {
		return nil, err
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	// Prepare download input
	input := &s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}

	if req.VersionID != "" {
		input.VersionId = aws.String(req.VersionID)
	}

	if req.Range != "" {
		input.Range = aws.String(req.Range)
	}

	// Track progress if callback is provided
	writer := req.Writer
	if callback != nil {
		// Get object info first to determine size
		headResult, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:    input.Bucket,
			Key:       input.Key,
			VersionId: input.VersionId,
		})
		if err != nil {
			return nil, ErrDownloadFailed.Wrap(err)
		}

		// Wrap the writer with progress tracking
		writer = &progressWriterAt{
			writer:   req.Writer,
			callback: callback,
			total:    headResult.ContentLength,
		}
	}

	// Perform download
	numBytes, err := c.downloader.Download(ctx, writer, input)
	if err != nil {
		return nil, ErrDownloadFailed.Wrap(err)
	}

	// Get object metadata
	headResult, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    input.Bucket,
		Key:       input.Key,
		VersionId: input.VersionId,
	})
	if err != nil {
		return nil, ErrDownloadFailed.Wrap(err)
	}

	return &DownloadResult{
		Size:         numBytes,
		ETag:         aws.ToString(headResult.ETag),
		LastModified: aws.ToTime(headResult.LastModified),
		ContentType:  aws.ToString(headResult.ContentType),
		Metadata:     headResult.Metadata,
	}, nil
}

// Close closes the S3 client and releases resources
func (c *client) Close() error {
	// AWS SDK v2 clients don't require explicit closing
	// This method is provided for interface compatibility
	return nil
}

// validateUploadRequest validates the upload request
func (c *client) validateUploadRequest(req *UploadRequest) error {
	if req == nil {
		return ErrInvalidRequest
	}

	if strings.TrimSpace(req.Bucket) == "" {
		return ErrMissingBucket
	}

	if strings.TrimSpace(req.Key) == "" {
		return ErrMissingKey
	}

	if req.Body == nil {
		return ErrMissingBody
	}

	return nil
}

// validateDownloadRequest validates the download request
func (c *client) validateDownloadRequest(req *DownloadRequest) error {
	if req == nil {
		return ErrInvalidRequest
	}

	if strings.TrimSpace(req.Bucket) == "" {
		return ErrMissingBucket
	}

	if strings.TrimSpace(req.Key) == "" {
		return ErrMissingKey
	}

	if req.Writer == nil {
		return ErrMissingWriter
	}

	return nil
}