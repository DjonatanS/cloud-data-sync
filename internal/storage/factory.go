package storage

import (
	"context"
	"fmt"
	"log/slog" // Import slog

	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
	"github.com/DjonatanS/cloud-data-sync/internal/providers/aws"
	"github.com/DjonatanS/cloud-data-sync/internal/providers/azure"
	"github.com/DjonatanS/cloud-data-sync/internal/providers/gcp"
	"github.com/DjonatanS/cloud-data-sync/internal/providers/minio"
)

// Factory manages storage provider instances
type Factory struct {
	providers map[string]interfaces.StorageProvider
	closers   []func() error
	logger    *slog.Logger // Add logger field
}

// NewFactory creates a new storage provider factory
func NewFactory(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Factory, error) { // Accept logger
	factory := &Factory{
		providers: make(map[string]interfaces.StorageProvider),
		logger:    logger.With("component", "storage_factory"), // Add component context
	}

	for _, providerCfg := range cfg.Providers {
		var provider interfaces.StorageProvider
		var err error

		factory.logger.Info("Initializing storage provider", "provider_id", providerCfg.ID, "provider_type", providerCfg.Type)

		switch providerCfg.Type {
		case config.GCS:
			if providerCfg.GCS == nil {
				return nil, fmt.Errorf("GCS provider %s has no configuration", providerCfg.ID)
			}
			provider, err = createGCSProvider(ctx, providerCfg)

		case config.MINIO:
			if providerCfg.MinIO == nil {
				return nil, fmt.Errorf("MinIO provider %s has no configuration", providerCfg.ID)
			}
			provider, err = createMinioProvider(providerCfg)

		case config.AWS:
			if providerCfg.AWS == nil {
				return nil, fmt.Errorf("AWS provider %s has no configuration", providerCfg.ID)
			}
			provider, err = createAWSProvider(providerCfg)

		case config.AZURE:
			if providerCfg.Azure == nil {
				return nil, fmt.Errorf("Azure provider %s has no configuration", providerCfg.ID)
			}
			provider, err = createAzureProvider(providerCfg)

		default:
			return nil, fmt.Errorf("unknown provider type: %s", providerCfg.Type)
		}

		if err != nil {
			factory.logger.Error("Failed to initialize provider", "provider_id", providerCfg.ID, "provider_type", providerCfg.Type, "error", err)
			// Close any previously initialized providers before returning error
			factory.Close()
			return nil, fmt.Errorf("error creating provider %s: %v", providerCfg.ID, err)
		}

		factory.providers[providerCfg.ID] = provider
		factory.logger.Info("Successfully initialized provider", "provider_id", providerCfg.ID, "provider_type", providerCfg.Type)
	}

	return factory, nil
}

// NewFactoryWithProviders creates a factory pre-populated with a provider map and logger (for testing)
func NewFactoryWithProviders(providers map[string]interfaces.StorageProvider, logger *slog.Logger) *Factory {
	return &Factory{
		providers: providers,
		logger:    logger.With("component", "storage_factory"),
	}
}

func (f *Factory) GetProvider(id string) (interfaces.StorageProvider, error) {
	provider, exists := f.providers[id]
	if !exists {
		return nil, fmt.Errorf("provider not found: %s", id)
	}
	return provider, nil
}

// Close cleans up resources used by the providers
func (f *Factory) Close() {
	f.logger.Info("Closing storage provider connections...")
	for _, provider := range f.providers {
		provider.Close()
	}
	f.logger.Info("Storage provider connections closed.")
}

func createGCSProvider(ctx context.Context, providerCfg config.ProviderConfig) (interfaces.StorageProvider, error) {
	clientConfig := gcp.Config{
		ProjectID: providerCfg.GCS.ProjectID,
	}

	return gcp.NewClient(ctx, clientConfig)
}

func createMinioProvider(providerCfg config.ProviderConfig) (interfaces.StorageProvider, error) {
	clientConfig := minio.Config{
		Endpoint:  providerCfg.MinIO.Endpoint,
		AccessKey: providerCfg.MinIO.AccessKey,
		SecretKey: providerCfg.MinIO.SecretKey,
		UseSSL:    providerCfg.MinIO.UseSSL,
		Region:    providerCfg.MinIO.Region,
	}

	return minio.NewClient(clientConfig)
}

func createAWSProvider(providerCfg config.ProviderConfig) (interfaces.StorageProvider, error) {
	clientConfig := aws.Config{
		Region:          providerCfg.AWS.Region,
		AccessKeyID:     providerCfg.AWS.AccessKeyID,
		SecretAccessKey: providerCfg.AWS.SecretAccessKey,
		Endpoint:        providerCfg.AWS.Endpoint,
		DisableSSL:      providerCfg.AWS.DisableSSL,
	}

	return aws.NewClient(clientConfig)
}

func createAzureProvider(providerCfg config.ProviderConfig) (interfaces.StorageProvider, error) {
	clientConfig := azure.Config{
		AccountName: providerCfg.Azure.AccountName,
		AccountKey:  providerCfg.Azure.AccountKey,
		EndpointURL: providerCfg.Azure.EndpointURL,
	}

	return azure.NewClient(clientConfig)
}
