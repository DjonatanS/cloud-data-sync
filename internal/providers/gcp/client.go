// Package gcp fornece a implementação da interface de armazenamento para Google Cloud Storage
package gcp

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
	"google.golang.org/api/iterator"
)

// Client implementa a interface StorageProvider para Google Cloud Storage
type Client struct {
	client    *storage.Client
	projectID string
}

// Config contém a configuração necessária para o cliente GCS
type Config struct {
	ProjectID string // Projeto responsável pela cobrança (requester pays)
}

// NewClient cria um novo cliente GCS
func NewClient(ctx context.Context, config Config) (*Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar cliente GCS: %v", err)
	}

	return &Client{
		client:    client,
		projectID: config.ProjectID,
	}, nil
}

// Close fecha o cliente GCS
func (c *Client) Close() error {
	return c.client.Close()
}

// ListObjects lista todos os objetos em um bucket específico no GCS
func (c *Client) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	// Configura o bucket com RequesterPays, se necessário
	bucket := c.client.Bucket(bucketName).UserProject(c.projectID)

	objects := make(map[string]*interfaces.ObjectInfo)
	query := &storage.Query{}
	it := bucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("erro ao iterar objetos do bucket %s: %v", bucketName, err)
		}

		objects[objAttrs.Name] = &interfaces.ObjectInfo{
			Name:         objAttrs.Name,
			Bucket:       bucketName,
			Size:         objAttrs.Size,
			ContentType:  objAttrs.ContentType,
			LastModified: objAttrs.Updated,
			ETag:         objAttrs.Etag,
			Metadata:     objAttrs.Metadata,
		}
	}

	return objects, nil
}

// GetObject obtém um objeto armazenado no GCS
func (c *Client) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	bucket := c.client.Bucket(bucketName).UserProject(c.projectID)
	obj := bucket.Object(objectName)

	// Obtém os atributos do objeto
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("erro ao obter atributos do objeto %s: %v", objectName, err)
	}

	// Converte para nossa estrutura ObjectInfo
	info := &interfaces.ObjectInfo{
		Name:         attrs.Name,
		Bucket:       bucketName,
		Size:         attrs.Size,
		ContentType:  attrs.ContentType,
		LastModified: attrs.Updated,
		ETag:         attrs.Etag,
		Metadata:     attrs.Metadata,
	}

	// Abre um leitor para o objeto
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return info, nil, fmt.Errorf("erro ao criar leitor para o objeto %s: %v", objectName, err)
	}

	return info, reader, nil
}

// UploadObject faz upload de um objeto para o GCS
func (c *Client) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	bucket := c.client.Bucket(bucketName).UserProject(c.projectID)
	obj := bucket.Object(objectName)

	wc := obj.NewWriter(ctx)
	wc.ContentType = contentType

	// Copia os dados do leitor para o writer
	written, err := io.Copy(wc, reader)
	if err != nil {
		wc.Close()
		return nil, fmt.Errorf("erro ao escrever objeto %s: %v", objectName, err)
	}

	// Fecha o writer para completar o upload
	if err := wc.Close(); err != nil {
		return nil, fmt.Errorf("erro ao finalizar upload do objeto %s: %v", objectName, err)
	}

	// Obtém os atributos do objeto após o upload
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("erro ao obter atributos após upload do objeto %s: %v", objectName, err)
	}

	return &interfaces.UploadInfo{
		Bucket: bucketName,
		Key:    objectName,
		ETag:   attrs.Etag,
		Size:   written,
	}, nil
}

// DeleteObject remove um objeto do GCS
func (c *Client) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	bucket := c.client.Bucket(bucketName).UserProject(c.projectID)
	obj := bucket.Object(objectName)

	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("erro ao remover objeto %s: %v", objectName, err)
	}

	return nil
}

// BucketExists verifica se um bucket existe no GCS
func (c *Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	bucket := c.client.Bucket(bucketName).UserProject(c.projectID)
	_, err := bucket.Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
	}
	return true, nil
}

// EnsureBucketExists garante que um bucket existe no GCS
func (c *Client) EnsureBucketExists(ctx context.Context, bucketName string) error {
	exists, err := c.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}

	if !exists {
		bucket := c.client.Bucket(bucketName).UserProject(c.projectID)
		if err := bucket.Create(ctx, c.projectID, nil); err != nil {
			return fmt.Errorf("erro ao criar bucket %s: %v", bucketName, err)
		}
	}

	return nil
}
