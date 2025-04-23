# Cloud Data Sync

A Go package for synchronizing data between different cloud storage providers. Supports Google Cloud Storage (GCS), Amazon S3, Azure Blob Storage, and MinIO (or any S3-compatible service).

## Overview

Cloud Data Sync is a tool that allows you to synchronize objects/files between different cloud storage providers. It is designed to be extensible, decoupled, and easy to use as a library or standalone application.

### Key Features

- Support for multiple storage providers:
  - Google Cloud Storage (GCS)
  - Amazon S3
  - Azure Blob Storage
  - MinIO (or any S3-compatible service)
- Unidirectional object synchronization (from a source to a destination)
- Metadata tracking for efficient synchronization
- Continuous synchronization with customizable interval
- On-demand single synchronization
- Change detection based on ETag and modification date
- Automatic removal of objects deleted at the source

## Installation

To install the package:

```sh
go get github.com/DjonatanS/cloud-data-sync
```

## Usage as a Library

### Basic Example

```go
package main

import (
	"context"
	"log"
	
	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/database"
	"github.com/DjonatanS/cloud-data-sync/internal/storage"
	"github.com/DjonatanS/cloud-data-sync/internal/sync"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}
	
	// Initialize context
	ctx := context.Background()
	
	// Initialize database
	db, err := database.NewDB(cfg.DatabasePath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()
	
	// Initialize provider factory
	factory, err := storage.NewFactory(ctx, cfg)
	if err != nil {
		log.Fatalf("Error initializing provider factory: %v", err)
	}
	defer factory.Close()
	
	// Create synchronizer
	synchronizer := sync.NewSynchronizer(db, cfg, factory)
	
	// Execute synchronization
	if err := synchronizer.SyncAll(ctx); err != nil {
		log.Fatalf("Error during synchronization: %v", err)
	}
}
```

### Implementing a New Provider

To add support for a new storage provider, implement the `storage.Provider` interface:

```go
// Example implementation for a new provider
package customstorage

import (
	"context"
	"io"
	
	"github.com/DjonatanS/cloud-data-sync/internal/storage"
)

type Client struct {
	// Provider-specific fields
}

func NewClient(config Config) (*Client, error) {
	// Client initialization
	return &Client{}, nil
}

func (c *Client) ListObjects(ctx context.Context, bucketName string) (map[string]*storage.ObjectInfo, error) {
	// Implementation for listing objects
}

func (c *Client) GetObject(ctx context.Context, bucketName, objectName string) (*storage.ObjectInfo, io.ReadCloser, error) {
	// Implementation for getting an object
}

func (c *Client) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*storage.UploadInfo, error) {
	// Implementation for uploading an object
}

// ... implementation of other interface methods
```

## Usage as an Application

### Compilation

```sh
go build -o cloud-data-sync ./cmd/gcs-minio-sync
```

### Configuration

Create a configuration file as shown in the example below or generate one with:

```sh
./cloud-data-sync --generate-config
```

Example configuration:

```json
{
  "databasePath": "data.db",
  "providers": [
    {
      "id": "gcs-bucket",
      "type": "gcs",
      "gcs": {
        "projectId": "your-gcp-project"
      }
    },
    {
      "id": "s3-storage",
      "type": "aws",
      "aws": {
        "region": "us-east-1",
        "accessKeyId": "your-access-key",
        "secretAccessKey": "your-secret-key"
      }
    },
    {
      "id": "azure-blob",
      "type": "azure",
      "azure": {
        "accountName": "your-azure-account",
        "accountKey": "your-azure-key"
      }
    },
    {
      "id": "local-minio",
      "type": "minio",
      "minio": {
        "endpoint": "localhost:9000",
        "accessKey": "minioadmin",
        "secretKey": "minioadmin",
        "useSSL": false
      }
    }
  ],
  "mappings": [
    {
      "sourceProviderId": "gcs-bucket",
      "sourceBucket": "source-bucket",
      "targetProviderId": "local-minio",
      "targetBucket": "destination-bucket"
    },
    {
      "sourceProviderId": "s3-storage",
      "sourceBucket": "source-bucket-s3",
      "targetProviderId": "azure-blob",
      "targetBucket": "destination-container-azure"
    }
  ]
}
```

### Execution

To run a single synchronization:

```sh
./cloud-data-sync --config config.json --once
```

To run the continuous service (periodic synchronization):

```sh
./cloud-data-sync --config config.json --interval 60
```

### Internal Packages

- **storage**: Defines the common interface for all storage providers.
  - **gcs**: Implementation of the interface for Google Cloud Storage.
  - **s3**: Implementation of the interface for Amazon S3.
  - **azure**: Implementation of the interface for Azure Blob Storage.
  - **minio**: Implementation of the interface for MinIO.
  
- **config**: Manages the application configuration.
- **database**: Provides metadata persistence for synchronization tracking.
- **sync**: Implements the synchronization logic between providers.

## Dependencies

- **Google Cloud Storage**: `cloud.google.com/go/storage`
- **AWS S3**: `github.com/aws/aws-sdk-go/service/s3`
- **Azure Blob**: `github.com/Azure/azure-storage-blob-go/azblob`
- **MinIO**: `github.com/minio/minio-go/v7`
- **SQLite**: `github.com/mattn/go-sqlite3`

## Requirements

- Go 1.18 or higher
- Valid credentials for the storage providers you want to use

## License

MIT

## Contributions

Contributions are welcome! Feel free to open issues or submit pull requests.

## Authors

- djonatan - Original author

## Next Updates

1. Memory and I/O optimization
   - Avoid reading the entire object into memory and then recreating `strings.NewReader(string(data))`. Instead, use `io.Pipe` or pass the `io.ReadCloser` directly for streaming upload.
   - Where buffering is still necessary, replace with `bytes.NewReader(data)` instead of converting to string:
     ```go
     // filepath: internal/sync/sync.go
     readerFromData := bytes.NewReader(data)
     _, err = targetProvider.UploadObject(
       ctx,
       mapping.TargetBucket,
       objName,
       readerFromData,
       int64(len(data)),
       srcObjInfo.ContentType,
     )
     ```

2. Parallelism and concurrency control
   - Process multiple objects in parallel (e.g., `errgroup.Group` + `semaphore.Weighted`) to increase throughput without exceeding API or memory limits.
   - Allow configuring the degree of concurrency per mapping in config.json.

3. Retry and fault tolerance
   - Implement a retry policy with backoff for network operations (List, Get, Upload, Delete), both generic and per provider.
   - Handle deadlines and use `ctx` in SDKs so that cancellation immediately stops operations.

4. Additional tests
   - Cover error scenarios in `SyncBuckets` (failure in `GetObject`, `UploadObject`, etc.) and ensure error counters and database status are updated correctly.
   - Create mocks with interfaces and use `gomock` or `testify/mock` to simulate failures and validate retry logic.

5. Observability
   - Expose metrics (Prometheus) for synchronized objects, latency, errors.
   - Add traces (OpenTelemetry) to track operations between providers and the DB.

6. Logging and levels
   - Consolidate logger calls: use `.Debug` for large payloads and flow details; `.Info` for milestones; `.Error` always with the error.
   - Allow configuring log level via flag.

7. Code quality and CI/CD
   - Add a GitHub Actions pipeline to run `go fmt`, `go vet`, `golangci-lint`, tests, and generate coverage.
   - Use semantic versioning modules for releases.

8. Configuration and extensibility
   - Support filters (prefix, regex) in each mapping.
   - Allow hooks before/after each sync (e.g., KMS keys, custom validations).

9. Full metadata handling
   - Preserve and propagate all object `Metadata` (not just `ContentType`), including headers and tags.
   - Add support for ACLs and encryption (when the provider offers it).

10. Graceful shutdown
    - Ensure that upon receiving a termination signal, wait for ongoing workers to finish or roll back.

With these improvements, the project will gain in performance, resilience, test coverage, and flexibility for growth.
