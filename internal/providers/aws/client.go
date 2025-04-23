// Package aws fornece a implementação da interface de armazenamento para Amazon S3
package aws

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Client implementa a interface StorageProvider para AWS S3
type Client struct {
	s3Client *s3.S3
}

// Config contém a configuração necessária para o cliente AWS S3
type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string // Opcional, para uso com serviços S3-compatible
	DisableSSL      bool   // Opcional, para uso com serviços S3-compatible
}

// NewClient cria um novo cliente AWS S3
func NewClient(config Config) (*Client, error) {
	// Configurações do AWS SDK
	awsConfig := &aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
	}

	// Configurar endpoint personalizado, se fornecido (para S3-compatible services)
	if config.Endpoint != "" {
		awsConfig.Endpoint = aws.String(config.Endpoint)
		awsConfig.DisableSSL = aws.Bool(config.DisableSSL)
		awsConfig.S3ForcePathStyle = aws.Bool(true) // Necessário para serviços compatíveis com S3
	}

	// Cria uma nova sessão AWS
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar sessão AWS: %v", err)
	}

	// Cria cliente S3
	s3Client := s3.New(sess)

	return &Client{s3Client: s3Client}, nil
}

// ListObjects lista todos os objetos em um bucket específico
func (c *Client) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	// Verifica se o bucket existe
	exists, err := c.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
	}
	if !exists {
		return make(map[string]*interfaces.ObjectInfo), nil
	}

	// Lista os objetos no bucket
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	objects := make(map[string]*interfaces.ObjectInfo)

	err = c.s3Client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			objects[*obj.Key] = &interfaces.ObjectInfo{
				Name:         *obj.Key,
				Bucket:       bucketName,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
				ETag:         aws.StringValue(obj.ETag),
			}
		}
		return true
	})

	if err != nil {
		return nil, fmt.Errorf("erro ao listar objetos do bucket %s: %v", bucketName, err)
	}

	// Para cada objeto, obtenha os metadados adicionais
	for key, object := range objects {
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}

		headOutput, err := c.s3Client.HeadObject(headInput)
		if err != nil {
			return nil, fmt.Errorf("erro ao obter metadados do objeto %s: %v", key, err)
		}

		object.ContentType = aws.StringValue(headOutput.ContentType)

		// Converter os metadados
		if headOutput.Metadata != nil {
			metadata := make(map[string]string)
			for k, v := range headOutput.Metadata {
				metadata[k] = aws.StringValue(v)
			}
			object.Metadata = metadata
		}
	}

	return objects, nil
}

// GetObject obtém um objeto armazenado no S3
func (c *Client) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	// Primeiro, obtém os metadados para construir o ObjectInfo
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	headOutput, err := c.s3Client.HeadObject(headInput)
	if err != nil {
		return nil, nil, fmt.Errorf("erro ao obter metadados do objeto %s: %v", objectName, err)
	}

	// Cria o objeto de informações
	info := &interfaces.ObjectInfo{
		Name:         objectName,
		Bucket:       bucketName,
		Size:         *headOutput.ContentLength,
		ContentType:  aws.StringValue(headOutput.ContentType),
		LastModified: *headOutput.LastModified,
		ETag:         aws.StringValue(headOutput.ETag),
	}

	// Converter os metadados
	if headOutput.Metadata != nil {
		metadata := make(map[string]string)
		for k, v := range headOutput.Metadata {
			metadata[k] = aws.StringValue(v)
		}
		info.Metadata = metadata
	}

	// Agora, obtém o conteúdo do objeto
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	output, err := c.s3Client.GetObject(input)
	if err != nil {
		return info, nil, fmt.Errorf("erro ao obter objeto %s: %v", objectName, err)
	}

	return info, output.Body, nil
}

// UploadObject faz upload de um objeto para o S3
func (c *Client) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	// Garante que o bucket existe
	if err := c.EnsureBucketExists(ctx, bucketName); err != nil {
		return nil, err
	}

	// Lê todo o conteúdo para um buffer
	// Note: isso pode ser melhorado para lidar com arquivos grandes sem carregar tudo na memória
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler conteúdo para upload: %v", err)
	}

	// Prepara o objeto para upload
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectName),
		Body:        bytes.NewReader(content),
		ContentType: aws.String(contentType),
	}

	// Faz o upload do objeto
	result, err := c.s3Client.PutObject(input)
	if err != nil {
		return nil, fmt.Errorf("erro ao fazer upload do objeto %s: %v", objectName, err)
	}

	return &interfaces.UploadInfo{
		Bucket: bucketName,
		Key:    objectName,
		ETag:   aws.StringValue(result.ETag),
		Size:   size,
	}, nil
}

// DeleteObject remove um objeto do S3
func (c *Client) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}

	_, err := c.s3Client.DeleteObject(input)
	if err != nil {
		return fmt.Errorf("erro ao remover objeto %s: %v", objectName, err)
	}

	return nil
}

// BucketExists verifica se um bucket existe no S3
func (c *Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err := c.s3Client.HeadBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound", "NoSuchBucket":
				return false, nil
			default:
				return false, fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
			}
		}
		return false, fmt.Errorf("erro ao verificar se o bucket %s existe: %v", bucketName, err)
	}

	return true, nil
}

// EnsureBucketExists garante que um bucket existe, criando-o se necessário
func (c *Client) EnsureBucketExists(ctx context.Context, bucketName string) error {
	exists, err := c.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}

	if !exists {
		createInput := &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		}

		_, err := c.s3Client.CreateBucket(createInput)
		if err != nil {
			return fmt.Errorf("erro ao criar bucket %s: %v", bucketName, err)
		}
	}

	return nil
}

// Close fecha o cliente (no caso do S3, não há nada a fechar)
func (c *Client) Close() error {
	return nil
}
