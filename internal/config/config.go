// Package config handles application configuration loading and validation.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type ProviderType string

const (
	GCS   ProviderType = "gcs"
	AWS   ProviderType = "aws"
	AZURE ProviderType = "azure"
	MINIO ProviderType = "minio"
)

// Config represents the application configuration including database path,
// storage providers, and bucket mappings.
type Config struct {
	DatabasePath string           `json:"databasePath"`
	Providers    []ProviderConfig `json:"providers"`
	Mappings     []BucketMapping  `json:"mappings"`
}

// ProviderConfig holds configuration for a specific storage provider.
type ProviderConfig struct {
	ID    string       `json:"id"`
	Type  ProviderType `json:"type"`
	GCS   *GCSConfig   `json:"gcs,omitempty"`
	AWS   *AWSConfig   `json:"aws,omitempty"`
	Azure *AzureConfig `json:"azure,omitempty"`
	MinIO *MinIOConfig `json:"minio,omitempty"`
}

// GCSConfig contains settings for Google Cloud Storage provider.
type GCSConfig struct {
	ProjectID string `json:"projectId"`
}

// AWSConfig contains settings for AWS S3 provider.
type AWSConfig struct {
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	Endpoint        string `json:"endpoint,omitempty"`
	DisableSSL      bool   `json:"disableSSL,omitempty"`
}

// AzureConfig contains settings for Azure Blob Storage provider.
type AzureConfig struct {
	AccountName string `json:"accountName"`
	AccountKey  string `json:"accountKey"`
	EndpointURL string `json:"endpointUrl,omitempty"`
}

// MinIOConfig contains settings for MinIO (S3-compatible) provider.
type MinIOConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	UseSSL    bool   `json:"useSSL"`
	Region    string `json:"region,omitempty"`
}

// BucketMapping defines a source-to-target bucket mapping for synchronization.
type BucketMapping struct {
	SourceProviderID string `json:"sourceProviderId"`
	SourceBucket     string `json:"sourceBucket"`
	TargetProviderID string `json:"targetProviderId"`
	TargetBucket     string `json:"targetBucket"`
}

// LoadConfig reads a JSON configuration file from the provided path,
// fills default values, and validates the resulting Config.
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "config.json"
	}

	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	if config.DatabasePath == "" {
		config.DatabasePath = "data.db"
	}

	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// validateConfig ensures that the given Config has at least one provider,
// unique provider IDs, and valid bucket mappings.
func validateConfig(config *Config) error {
	if len(config.Providers) == 0 {
		return fmt.Errorf("configuration must contain at least one provider")
	}

	idMap := make(map[string]bool)
	for _, provider := range config.Providers {
		if idMap[provider.ID] {
			return fmt.Errorf("duplicate provider ID: %s", provider.ID)
		}
		idMap[provider.ID] = true

		switch provider.Type {
		case GCS:
			if provider.GCS == nil {
				return fmt.Errorf("GCS provider %s has no configuration", provider.ID)
			}
		case AWS:
			if provider.AWS == nil {
				return fmt.Errorf("AWS provider %s has no configuration", provider.ID)
			}
		case AZURE:
			if provider.Azure == nil {
				return fmt.Errorf("Azure provider %s has no configuration", provider.ID)
			}
		case MINIO:
			if provider.MinIO == nil {
				return fmt.Errorf("MinIO provider %s has no configuration", provider.ID)
			}
		default:
			return fmt.Errorf("unknown provider type: %s", provider.Type)
		}
	}

	if len(config.Mappings) == 0 {
		return fmt.Errorf("configuration must contain at least one bucket mapping")
	}

	for i, mapping := range config.Mappings {
		if !idMap[mapping.SourceProviderID] {
			return fmt.Errorf("mapping %d uses non-existent source provider: %s", i, mapping.SourceProviderID)
		}
		if !idMap[mapping.TargetProviderID] {
			return fmt.Errorf("mapping %d uses non-existent target provider: %s", i, mapping.TargetProviderID)
		}
	}

	return nil
}

// SaveDefaultConfig writes a default JSON configuration file to the given path.
func SaveDefaultConfig(configPath string) error {
	config := &Config{
		DatabasePath: "data.db",
		Providers: []ProviderConfig{
			{
				ID:   "gcp",
				Type: GCS,
				GCS: &GCSConfig{
					ProjectID: "your-gcp-project",
				},
			},
			{
				ID:   "minio",
				Type: MINIO,
				MinIO: &MinIOConfig{
					Endpoint:  "localhost:9000",
					AccessKey: "minioadmin",
					SecretKey: "minioadmin",
					UseSSL:    false,
				},
			},
			{
				ID:   "aws",
				Type: AWS,
				AWS: &AWSConfig{
					Region:          "us-east-1",
					AccessKeyID:     "your-access-key-id",
					SecretAccessKey: "your-secret-access-key",
				},
			},
			{
				ID:   "azure",
				Type: AZURE,
				Azure: &AzureConfig{
					AccountName: "your-azure-account",
					AccountKey:  "your-azure-key",
				},
			},
		},
		Mappings: []BucketMapping{
			{
				SourceProviderID: "gcs-example",
				SourceBucket:     "gcs-source-bucket",
				TargetProviderID: "minio-local",
				TargetBucket:     "minio-target-bucket",
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(configPath, data, 0644)
}
