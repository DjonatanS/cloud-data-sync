package interfaces

import (
	"context"
	"io"
	"time"
)

type ObjectInfo struct {
	Name         string
	Bucket       string
	Size         int64
	ContentType  string
	LastModified time.Time
	ETag         string
	Metadata     map[string]string
}

type UploadInfo struct {
	Bucket string
	Key    string
	ETag   string
	Size   int64
}

type StorageProvider interface {
	ListObjects(ctx context.Context, bucketName string) (map[string]*ObjectInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string) (*ObjectInfo, io.ReadCloser, error)
	UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*UploadInfo, error)
	DeleteObject(ctx context.Context, bucketName, objectName string) error
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	EnsureBucketExists(ctx context.Context, bucketName string) error
	Close() error
}
