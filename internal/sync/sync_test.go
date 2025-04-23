package sync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"log/slog"

	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/database"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
	"github.com/DjonatanS/cloud-data-sync/internal/storage"
)

// fakeSourceProvider implements StorageProvider for source, returns predefined objects and data
type fakeSourceProvider struct {
	objects map[string]*interfaces.ObjectInfo
	data    map[string][]byte
}

func (f *fakeSourceProvider) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	return f.objects, nil
}
func (f *fakeSourceProvider) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	info := f.objects[objectName]
	return info, io.NopCloser(bytes.NewReader(f.data[objectName])), nil
}
func (f *fakeSourceProvider) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	// not used
	return nil, nil
}
func (f *fakeSourceProvider) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	return nil
}
func (f *fakeSourceProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return true, nil
}
func (f *fakeSourceProvider) EnsureBucketExists(ctx context.Context, bucketName string) error {
	return nil
}
func (f *fakeSourceProvider) Close() error { return nil }

// fakeTargetProvider captures uploaded objects
type fakeTargetProvider struct {
	uploaded map[string][]byte
	objects  map[string]*interfaces.ObjectInfo // initial target objects
	deleted  []string
}

func (f *fakeTargetProvider) ListObjects(ctx context.Context, bucketName string) (map[string]*interfaces.ObjectInfo, error) {
	return f.objects, nil
}
func (f *fakeTargetProvider) GetObject(ctx context.Context, bucketName, objectName string) (*interfaces.ObjectInfo, io.ReadCloser, error) {
	// not used
	return nil, nil, nil
}
func (f *fakeTargetProvider) UploadObject(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64, contentType string) (*interfaces.UploadInfo, error) {
	buf := new(bytes.Buffer)
	io.Copy(buf, reader)
	f.uploaded[objectName] = buf.Bytes()
	return &interfaces.UploadInfo{Bucket: bucketName, Key: objectName, Size: size}, nil
}
func (f *fakeTargetProvider) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	f.deleted = append(f.deleted, objectName)
	return nil
}
func (f *fakeTargetProvider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return true, nil
}
func (f *fakeTargetProvider) EnsureBucketExists(ctx context.Context, bucketName string) error {
	return nil
}
func (f *fakeTargetProvider) Close() error { return nil }

func TestSyncBuckets_CopyAndMetadata(t *testing.T) {
	// Setup fake providers with one object
	now := time.Now().UTC().Truncate(time.Second)
	srcObjects := map[string]*interfaces.ObjectInfo{
		"file1.txt": {Name: "file1.txt", Bucket: "src", Size: 4, ContentType: "text/plain", LastModified: now, ETag: "etag1", Metadata: nil},
	}
	srcData := map[string][]byte{"file1.txt": []byte("data")}
	source := &fakeSourceProvider{objects: srcObjects, data: srcData}
	target := &fakeTargetProvider{uploaded: make(map[string][]byte), objects: map[string]*interfaces.ObjectInfo{}}

	// Prepare DB and config
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/sync.db"
	db, err := database.NewDB(dbPath)
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	cfg := &config.Config{Mappings: []config.BucketMapping{{SourceProviderID: "src", SourceBucket: "src", TargetProviderID: "tgt", TargetBucket: "tgt"}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Use factory pre-populated with fake providers for testing
	factory := storage.NewFactoryWithProviders(
		map[string]interfaces.StorageProvider{"src": source, "tgt": target},
		logger,
	)

	syncer := NewSynchronizer(db, cfg, factory, logger)

	// Execute SyncBuckets
	err = syncer.SyncBuckets(context.Background(), cfg.Mappings[0], logger)
	if err != nil {
		t.Fatalf("SyncBuckets returned error: %v", err)
	}

	// Verify upload happened
	if data, ok := target.uploaded["file1.txt"]; !ok || !bytes.Equal(data, []byte("data")) {
		t.Errorf("expected uploaded data 'data', got %v", data)
	}

	// Verify metadata in DB
	mappingID := "src:src->tgt:tgt"
	meta, err := db.GetFileMetadata(mappingID, "file1.txt")
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}
	if meta == nil || meta.ETag != "etag1" || meta.SyncStatus != "success" {
		t.Errorf("unexpected metadata: %+v", meta)
	}
}
