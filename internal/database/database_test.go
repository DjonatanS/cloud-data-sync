package database

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDB_UpsertGetDeleteFileMetadata(t *testing.T) {
	// Create temporary database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	mappingID := "m1"
	objectName := "obj1"
	fm := &FileMetadata{
		MappingID:    mappingID,
		ObjectName:   objectName,
		Size:         123,
		LastModified: time.Now().UTC().Truncate(time.Second),
		ETag:         "etag1",
		ContentType:  "text/plain",
		LastSynced:   time.Now().UTC().Truncate(time.Second),
		SyncStatus:   "success",
	}

	// Upsert metadata
	if err := db.UpsertFileMetadata(fm); err != nil {
		t.Fatalf("UpsertFileMetadata failed: %v", err)
	}

	// Get metadata
	got, err := db.GetFileMetadata(mappingID, objectName)
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}
	if got == nil {
		t.Fatal("expected metadata, got nil")
	}
	if got.MappingID != fm.MappingID || got.ObjectName != fm.ObjectName || got.Size != fm.Size || got.ETag != fm.ETag || got.SyncStatus != fm.SyncStatus {
		t.Errorf("got metadata %+v, want %+v", got, fm)
	}

	// Delete metadata
	if err := db.DeleteFileMetadata(mappingID, objectName); err != nil {
		t.Fatalf("DeleteFileMetadata failed: %v", err)
	}

	// Get after delete
	got, err = db.GetFileMetadata(mappingID, objectName)
	if err != nil {
		t.Fatalf("GetFileMetadata after delete failed: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no metadata after delete, got %+v", got)
	}
}

func TestDB_ListFileMetadataByMapping(t *testing.T) {
	// Create temporary database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test2.db")
	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	mappingID := "mapA"
	objects := []string{"a", "b", "c"}
	for _, name := range objects {
		fm := &FileMetadata{
			MappingID:    mappingID,
			ObjectName:   name,
			Size:         int64(len(name)),
			LastModified: time.Now().UTC().Truncate(time.Second),
			ETag:         "etag",
			ContentType:  "ct",
			LastSynced:   time.Now().UTC().Truncate(time.Second),
			SyncStatus:   "success",
		}
		if err := db.UpsertFileMetadata(fm); err != nil {
			t.Fatalf("UpsertFileMetadata failed: %v", err)
		}
	}

	list, err := db.ListFileMetadataByMapping(mappingID)
	if err != nil {
		t.Fatalf("ListFileMetadataByMapping failed: %v", err)
	}
	if len(list) != len(objects) {
		t.Errorf("expected %d entries, got %d", len(objects), len(list))
	}
}
