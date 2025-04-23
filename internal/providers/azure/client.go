package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
)

type Client struct {
	containerURL azblob.ContainerURL
	serviceURL   azblob.ServiceURL
	credential   azblob.SharedKeyCredential
}

type Config struct {
	AccountName string
	AccountKey  string
	EndpointURL string
}

func NewClient(config Config) (*Client, error) {
	credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
	if err != nil {
		return nil, fmt.Errorf("error creating Azure credentials: %v", err)
	}

	endpointURL := config.EndpointURL
	if endpointURL == "" {
		endpointURL = fmt.Sprintf("https://%s.blob.core.windows.net", config.AccountName)
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	serviceURL, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing endpoint URL: %v", err)
	}

	azServiceURL := azblob.NewServiceURL(*serviceURL, pipeline)

	return &Client{
		serviceURL: azServiceURL,
		credential: *credential,
	}, nil
}

func (c *Client) getContainerURL(containerName string) azblob.ContainerURL {
	return c.serviceURL.NewContainerURL(containerName)
}

func (c *Client) getBlobURL(containerName, blobName string) azblob.BlockBlobURL {
	containerURL := c.getContainerURL(containerName)
	return containerURL.NewBlockBlobURL(blobName)
}

func (c *Client) ListObjects(ctx context.Context, containerName string) (map[string]*interfaces.ObjectInfo, error) {
	exists, err := c.BucketExists(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("error checking if container %s exists: %v", containerName, err)
	}
	if !exists {
		return make(map[string]*interfaces.ObjectInfo), nil
	}

	containerURL := c.getContainerURL(containerName)
	objects := make(map[string]*interfaces.ObjectInfo)

	options := azblob.ListBlobsSegmentOptions{
		Details: azblob.BlobListingDetails{
			Metadata: true,
		},
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		response, err := containerURL.ListBlobsFlatSegment(ctx, marker, options)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs from container %s: %v", containerName, err)
		}

		marker = response.NextMarker

		for _, blob := range response.Segment.BlobItems {
			contentType := ""
			if blob.Properties.ContentType != nil {
				contentType = *blob.Properties.ContentType
			}
			objects[blob.Name] = &interfaces.ObjectInfo{
				Name:         blob.Name,
				Bucket:       containerName,
				Size:         *blob.Properties.ContentLength,
				ContentType:  contentType,
				LastModified: blob.Properties.LastModified,
				ETag:         string(blob.Properties.Etag),
				Metadata:     blob.Metadata,
			}
		}
	}

	return objects, nil
}

func (c *Client) GetObject(ctx context.Context, containerName, blobName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	blobURL := c.getBlobURL(containerName, blobName)

	props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("error getting blob %s properties: %v", blobName, err)
	}

	info := &interfaces.ObjectInfo{
		Name:         blobName,
		Bucket:       containerName,
		Size:         props.ContentLength(),
		ContentType:  props.ContentType(),
		LastModified: props.LastModified(),
		ETag:         string(props.ETag()),
		Metadata:     props.NewMetadata(),
	}

	response, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return info, nil, fmt.Errorf("error downloading blob %s: %v", blobName, err)
	}

	return info, response.Body(azblob.RetryReaderOptions{}), nil
}

func (c *Client) UploadObject(ctx context.Context, containerName, blobName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	if err := c.EnsureBucketExists(ctx, containerName); err != nil {
		return nil, err
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading content for upload: %v", err)
	}

	blobURL := c.getBlobURL(containerName, blobName)

	options := azblob.UploadToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{
			ContentType: contentType,
		},
	}

	response, err := azblob.UploadBufferToBlockBlob(ctx, content, blobURL, options)
	if err != nil {
		return nil, fmt.Errorf("error uploading blob %s: %v", blobName, err)
	}

	return &interfaces.UploadInfo{
		Bucket: containerName,
		Key:    blobName,
		ETag:   string(response.ETag()),
		Size:   int64(len(content)),
	}, nil
}

func (c *Client) DeleteObject(ctx context.Context, containerName, blobName string) error {
	blobURL := c.getBlobURL(containerName, blobName)

	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok && stgErr.Response().StatusCode == 404 {
			return nil
		}
		return fmt.Errorf("error deleting blob %s: %v", blobName, err)
	}

	return nil
}

func (c *Client) BucketExists(ctx context.Context, containerName string) (bool, error) {
	containerURL := c.getContainerURL(containerName)

	_, err := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); ok && stgErr.Response().StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("error checking if container %s exists: %v", containerName, err)
	}

	return true, nil
}

func (c *Client) EnsureBucketExists(ctx context.Context, containerName string) error {
	exists, err := c.BucketExists(ctx, containerName)
	if err != nil {
		return err
	}

	if !exists {
		containerURL := c.getContainerURL(containerName)

		_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		if err != nil {
			if stgErr, ok := err.(azblob.StorageError); ok && stgErr.ServiceCode() == azblob.ServiceCodeContainerAlreadyExists {
				return nil
			}
			return fmt.Errorf("error creating container %s: %v", containerName, err)
		}
	}

	return nil
}

func (c *Client) Close() error {
	return nil
}
