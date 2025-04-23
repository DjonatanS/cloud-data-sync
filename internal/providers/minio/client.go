// Package minio fornece a implementação da interface de armazenamento para MinIO/S3-compatible
package minio

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
)

type Client struct {
	client *minio.Client
}

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
}

func NewClient(config Config) (*Client, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("erro ao criar cliente MinIO: %v", err)
	}

	return &Client{client: client}, nil
}

func (c *Client) EnsureBucketExists(ctx context.Context, bucketName string) error {
	exists, err := c.client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
	}

	if !exists {
		log.Printf("Bucket '%s' não existe, criando...", bucketName)
		err = c.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("erro ao criar o bucket %s: %v", bucketName, err)
		}
		log.Printf("Bucket '%s' criado com sucesso", bucketName)
	}

	return nil
}

func (c *Client) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	objects := make(map[string]*interfaces.ObjectInfo)

	exists, err := c.client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
	}

	if !exists {
		return objects, nil
	}

	objectCh := c.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("erro ao listar objetos do bucket %s: %v", bucketName, object.Err)
		}

		objects[object.Key] = &interfaces.ObjectInfo{
			Name:         object.Key,
			Bucket:       bucketName,
			Size:         object.Size,
			ContentType:  object.ContentType,
			LastModified: object.LastModified,
			ETag:         object.ETag,
			Metadata:     object.UserMetadata,
		}
	}

	return objects, nil
}

func (c *Client) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	objInfo, err := c.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("erro ao obter metadados do objeto %s: %v", objectName, err)
	}

	info := &interfaces.ObjectInfo{
		Name:         objInfo.Key,
		Bucket:       bucketName,
		Size:         objInfo.Size,
		ContentType:  objInfo.ContentType,
		LastModified: objInfo.LastModified,
		ETag:         objInfo.ETag,
		Metadata:     objInfo.UserMetadata,
	}

	reader, err := c.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return info, nil, fmt.Errorf("erro ao obter objeto %s: %v", objectName, err)
	}

	return info, reader, nil
}

func (c *Client) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	err := c.EnsureBucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("erro ao garantir que o bucket %s existe: %v", bucketName, err)
	}

	info, err := c.client.PutObject(ctx, bucketName, objectName, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})

	if err != nil {
		return nil, fmt.Errorf("erro ao fazer upload do objeto %s: %v", objectName, err)
	}

	return &interfaces.UploadInfo{
		Bucket: bucketName,
		Key:    objectName,
		ETag:   info.ETag,
		Size:   info.Size,
	}, nil
}

func (c *Client) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	err := c.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("erro ao remover objeto %s: %v", objectName, err)
	}

	return nil
}

func (c *Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return c.client.BucketExists(ctx, bucketName)
}

func (c *Client) Close() error {
	return nil
}
