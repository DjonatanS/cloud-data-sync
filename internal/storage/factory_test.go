package storage

import (
	"context"
	"io"
	"testing"

	"log/slog"

	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
)

// fakeProvider implements StorageProvider for testing GetProvider
// and simulates no-op behavior
type fakeProvider struct{}

func (f *fakeProvider) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	return nil, nil
}
func (f *fakeProvider) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	return nil, nil, nil
}
func (f *fakeProvider) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	return nil, nil
}
func (f *fakeProvider) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	return nil
}
func (f *fakeProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return true, nil
}
func (f *fakeProvider) EnsureBucketExists(ctx context.Context, bucketName string) error {
	return nil
}
func (f *fakeProvider) Close() error {
	return nil
}

func TestFactory_GetProvider_Success(t *testing.T) {
	providerMap := map[string]interfaces.StorageProvider{"p1": &fakeProvider{}}
	factory := &Factory{providers: providerMap, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p, err := factory.GetProvider("p1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p == nil {
		t.Fatal("expected provider, got nil")
	}
}

func TestFactory_GetProvider_NotFound(t *testing.T) {
	factory := &Factory{providers: map[string]interfaces.StorageProvider{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	_, err := factory.GetProvider("missing")
	if err == nil {
		t.Fatal("expected error for missing provider, got nil")
	}
}

func TestNewFactory_UnknownType(t *testing.T) {
	cfg := &config.Config{Providers: []config.ProviderConfig{{ID: "x", Type: config.ProviderType("unknown")}}}
	_, err := NewFactory(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected error for unknown provider type, got nil")
	}
}
