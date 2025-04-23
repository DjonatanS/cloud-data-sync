// Package sync provides functionalities to synchronize data between different storage providers
package sync

import (
	"context"
	"fmt"
	"log/slog" // Import slog
	"time"

	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/database"
	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
	"github.com/DjonatanS/cloud-data-sync/internal/storage"
)

type Synchronizer struct {
	db              *database.DB
	config          *config.Config
	providerFactory *storage.Factory
	logger          *slog.Logger
}

func NewSynchronizer(db *database.DB, cfg *config.Config, factory *storage.Factory, logger *slog.Logger) *Synchronizer { // Accept logger
	return &Synchronizer{
		db:              db,
		config:          cfg,
		providerFactory: factory,
		logger:          logger,
	}
}

func (s *Synchronizer) SyncAll(ctx context.Context) error {
	for _, mapping := range s.config.Mappings {
		mapLogger := s.logger.With(
			"source_provider", mapping.SourceProviderID,
			"source_bucket", mapping.SourceBucket,
			"target_provider", mapping.TargetProviderID,
			"target_bucket", mapping.TargetBucket,
		)
		mapLogger.Info("Starting synchronization for mapping")

		err := s.SyncBuckets(ctx, mapping, mapLogger) // Pass logger down
		if err != nil {
			mapLogger.Error("Error synchronizing mapping", "error", err)
			// Continue with the next mapping even in case of error
			continue
		}

		mapLogger.Info("Synchronization mapping completed successfully")
	}

	return nil
}

// SyncBuckets synchronizes a specific mapping between buckets
func (s *Synchronizer) SyncBuckets(ctx context.Context, mapping config.BucketMapping, logger *slog.Logger) error { // Accept logger
	sourceProvider, err := s.providerFactory.GetProvider(mapping.SourceProviderID)
	if err != nil {
		logger.Error("Failed to get source provider", "error", err)
		return fmt.Errorf("error getting source provider %s: %w", mapping.SourceProviderID, err)
	}

	targetProvider, err := s.providerFactory.GetProvider(mapping.TargetProviderID)
	if err != nil {
		logger.Error("Failed to get target provider", "error", err)
		return fmt.Errorf("error getting target provider %s: %w", mapping.TargetProviderID, err)
	}

	logger.Debug("Listing objects from source bucket") // Use Debug for finer-grained logs
	sourceObjects, err := sourceProvider.ListObjects(ctx, mapping.SourceBucket)
	if err != nil {
		logger.Error("Failed to list objects from source bucket", "error", err)
		return fmt.Errorf("error listing objects from source bucket %s: %w", mapping.SourceBucket, err)
	}
	logger.Debug("Listed source objects", "count", len(sourceObjects))

	logger.Debug("Listing objects from target bucket")
	targetObjects, err := targetProvider.ListObjects(ctx, mapping.TargetBucket)
	if err != nil {
		// Log warning instead of error? Maybe bucket doesn't exist yet.
		logger.Warn("Failed to list objects from target bucket, attempting to ensure bucket exists", "error", err)
		// Continue, EnsureBucketExists will handle creation or return error
	} else {
		logger.Debug("Listed target objects", "count", len(targetObjects))
	}

	logger.Debug("Ensuring target bucket exists")
	if err := targetProvider.EnsureBucketExists(ctx, mapping.TargetBucket); err != nil {
		logger.Error("Failed to ensure target bucket exists", "error", err)
		return fmt.Errorf("error ensuring target bucket %s exists: %w", mapping.TargetBucket, err)
	}

	mappingID := fmt.Sprintf("%s:%s->%s:%s",
		mapping.SourceProviderID, mapping.SourceBucket,
		mapping.TargetProviderID, mapping.TargetBucket)

	syncCounter := 0
	skipCounter := 0
	errorCounter := 0

	for objName, srcObjInfo := range sourceObjects {
		objLogger := logger.With("object_name", objName) // Logger with object context
		objLogger.Debug("Processing object")

		storedMetadata, err := s.db.GetFileMetadata(mappingID, objName)
		if err != nil {
			// Log error but continue, treat as if metadata doesn't exist
			objLogger.Warn("Error fetching metadata from DB, proceeding as if object is new/changed", "error", err)
		}

		needsSync := true
		if storedMetadata != nil {
			// Compare metadata
			if storedMetadata.LastModified.Equal(srcObjInfo.LastModified) && storedMetadata.ETag == srcObjInfo.ETag && storedMetadata.SyncStatus == "success" {
				needsSync = false
				objLogger.Debug("Object metadata matches and last sync succeeded, skipping",
					"db_last_modified", storedMetadata.LastModified, "src_last_modified", srcObjInfo.LastModified,
					"db_etag", storedMetadata.ETag, "src_etag", srcObjInfo.ETag)
				skipCounter++
			} else {
				objLogger.Info("Object changed or previous sync failed, needs sync",
					"db_last_modified", storedMetadata.LastModified, "src_last_modified", srcObjInfo.LastModified,
					"db_etag", storedMetadata.ETag, "src_etag", srcObjInfo.ETag,
					"db_sync_status", storedMetadata.SyncStatus)
			}
		} else {
			objLogger.Info("Object not found in DB or failed to fetch metadata, needs sync")
		}

		if needsSync {
			objLogger.Info("Synchronizing object")

			objLogger.Debug("Getting object from source")
			_, reader, err := sourceProvider.GetObject(ctx, mapping.SourceBucket, objName)
			if err != nil {
				objLogger.Error("Error getting object from source", "error", err)
				s.updateObjectMetadata(mappingID, objName, srcObjInfo, "failed_get", objLogger)
				errorCounter++
				continue
			}
			// garante fechamento ao final
			defer reader.Close()

			objLogger.Debug("Uploading object to target (stream)", "size", srcObjInfo.Size, "content_type", srcObjInfo.ContentType)
			_, err = targetProvider.UploadObject(
				ctx,
				mapping.TargetBucket,
				objName,
				reader,          // leio direto do ReadCloser
				srcObjInfo.Size, // tamanho já conhecido em srcObjInfo
				srcObjInfo.ContentType,
			)
			if err != nil {
				objLogger.Error("Error uploading object to target", "error", err)
				s.updateObjectMetadata(mappingID, objName, srcObjInfo, "failed_upload", objLogger)
				errorCounter++
				continue
			}

			objLogger.Info("Object synchronized successfully")
			s.updateObjectMetadata(mappingID, objName, srcObjInfo, "success", objLogger)
			syncCounter++
		}
	}

	logger.Info("Object synchronization phase complete",
		"synced", syncCounter,
		"skipped", skipCounter,
		"errors", errorCounter,
		"total_source_objects", len(sourceObjects))

	// Pass logger to removeDeletedObjects
	s.removeDeletedObjects(ctx, mappingID, mapping, sourceObjects, targetObjects, targetProvider, logger)

	return nil
}

// updateObjectMetadata updates object metadata in the database
func (s *Synchronizer) updateObjectMetadata(mappingID string, objectName string, info *interfaces.ObjectInfo, status string, logger *slog.Logger) { // Accept logger
	metadata := &database.FileMetadata{
		MappingID:    mappingID,
		ObjectName:   objectName,
		Size:         info.Size,
		LastModified: info.LastModified,
		ETag:         info.ETag,
		ContentType:  info.ContentType,
		LastSynced:   time.Now().UTC(), // Use UTC
		SyncStatus:   status,
	}

	logger.Debug("Upserting file metadata", "status", status)
	if err := s.db.UpsertFileMetadata(metadata); err != nil {
		// Use the passed-in object-specific logger
		logger.Error("Error updating metadata in DB", "error", err)
	}
}

// removeDeletedObjects removes objects from the target that no longer exist in the source
func (s *Synchronizer) removeDeletedObjects(
	ctx context.Context,
	mappingID string,
	mapping config.BucketMapping,
	sourceObjects map[string]*interfaces.ObjectInfo,
	targetObjects map[string]*interfaces.ObjectInfo,
	targetProvider interfaces.StorageProvider,
	logger *slog.Logger, // Accept logger
) {
	logger.Info("Checking for objects to remove from target")
	deleteCounter := 0
	errorCounter := 0

	for objName := range targetObjects {
		if _, exists := sourceObjects[objName]; !exists {
			objLogger := logger.With("object_name", objName) // Logger with object context
			objLogger.Info("Removing object from target (deleted from source)")

			if err := targetProvider.DeleteObject(ctx, mapping.TargetBucket, objName); err != nil {
				objLogger.Error("Error removing object from target", "error", err)
				errorCounter++
				continue // Skip DB deletion if target deletion failed
			}

			objLogger.Debug("Removing object metadata from DB")
			if err := s.db.DeleteFileMetadata(mappingID, objName); err != nil {
				objLogger.Error("Error removing metadata from DB", "error", err)
				// Log error but continue, object was deleted from target
			}

			objLogger.Info("Object removed successfully from target")
			deleteCounter++
		}
	}
	logger.Info("Object removal phase complete", "removed", deleteCounter, "errors", errorCounter)
}
