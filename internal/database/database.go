package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const currentSchemaVersion = 2

type FileMetadata struct {
	ID           int64
	MappingID    string
	ObjectName   string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	LastSynced   time.Time
	SyncStatus   string
}

type DB struct {
	db *sql.DB
}

func NewDB(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	dbInstance := &DB{db: db}

	if err := dbInstance.initializeSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error initializing schema: %v", err)
	}

	return dbInstance, nil
}

func (db *DB) initializeSchema() error {
	var tableExists bool
	err := db.db.QueryRow(`
		SELECT COUNT(*) > 0 FROM sqlite_master 
		WHERE type='table' AND name='schema_migrations'
	`).Scan(&tableExists)

	if err != nil {
		return fmt.Errorf("error checking migrations table: %v", err)
	}

	var version int
	if !tableExists {
		_, err := db.db.Exec(`
			CREATE TABLE schema_migrations (
				version INTEGER PRIMARY KEY,
				applied_at TIMESTAMP NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating migrations table: %v", err)
		}
		version = 0
	} else {
		err := db.db.QueryRow(`
			SELECT COALESCE(MAX(version), 0) FROM schema_migrations
		`).Scan(&version)

		if err != nil {
			return fmt.Errorf("error getting schema version: %v", err)
		}
	}

	return db.applyMigrations(version)
}

func (db *DB) applyMigrations(currentVersion int) error {
	if currentVersion >= currentSchemaVersion {
		return nil
	}

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction for migrations: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for v := currentVersion + 1; v <= currentSchemaVersion; v++ {
		if err := db.applyMigration(tx, v); err != nil {
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO schema_migrations (version, applied_at) 
			VALUES (?, CURRENT_TIMESTAMP)
		`, v)
		if err != nil {
			return fmt.Errorf("error recording migration %d: %v", v, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing migrations: %v", err)
	}

	return nil
}

func (db *DB) applyMigration(tx *sql.Tx, version int) error {
	var err error

	switch version {
	case 1:
		var hasBucketName, hasSourceBucket bool

		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('file_metadata') 
			WHERE name='bucket_name'
		`).Scan(&hasBucketName)

		if err != nil {
			return fmt.Errorf("error checking bucket_name column existence: %v", err)
		}

		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('file_metadata') 
			WHERE name='source_bucket'
		`).Scan(&hasSourceBucket)

		if err != nil {
			return fmt.Errorf("error checking source_bucket column existence: %v", err)
		}

		if !hasBucketName && !hasSourceBucket {
			_, err = tx.Exec(`
				CREATE TABLE IF NOT EXISTS file_metadata (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					mapping_id TEXT NOT NULL,
					object_name TEXT NOT NULL,
					size INTEGER NOT NULL,
					last_modified TIMESTAMP NOT NULL,
					etag TEXT,
					content_type TEXT,
					last_synced TIMESTAMP NOT NULL,
					sync_status TEXT NOT NULL,
					UNIQUE(mapping_id, object_name)
				);
				CREATE INDEX IF NOT EXISTS idx_file_metadata_mapping_object 
				ON file_metadata(mapping_id, object_name);
			`)
		}

	case 2:
		var tableExists bool
		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM sqlite_master 
			WHERE type='table' AND name='file_metadata'
		`).Scan(&tableExists)

		if err != nil {
			return fmt.Errorf("error checking file_metadata table existence: %v", err)
		}

		if !tableExists {
			_, err = tx.Exec(`
				CREATE TABLE file_metadata (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					mapping_id TEXT NOT NULL,
					object_name TEXT NOT NULL,
					size INTEGER NOT NULL,
					last_modified TIMESTAMP NOT NULL,
					etag TEXT,
					content_type TEXT,
					last_synced TIMESTAMP NOT NULL,
					sync_status TEXT NOT NULL,
					UNIQUE(mapping_id, object_name)
				);
				CREATE INDEX idx_file_metadata_mapping_object 
				ON file_metadata(mapping_id, object_name);
			`)
			return err
		}

		var hasMappingID, hasBucketName, hasSourceBucket bool

		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('file_metadata') 
			WHERE name='mapping_id'
		`).Scan(&hasMappingID)

		if err != nil {
			return fmt.Errorf("error checking mapping_id column existence: %v", err)
		}

		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('file_metadata') 
			WHERE name='bucket_name'
		`).Scan(&hasBucketName)

		if err != nil {
			return fmt.Errorf("error checking bucket_name column existence: %v", err)
		}

		err = tx.QueryRow(`
			SELECT COUNT(*) > 0 FROM pragma_table_info('file_metadata') 
			WHERE name='source_bucket'
		`).Scan(&hasSourceBucket)

		if err != nil {
			return fmt.Errorf("error checking source_bucket column existence: %v", err)
		}

		if hasMappingID {
			return nil
		}

		if hasBucketName {
			_, err = tx.Exec(`
				-- Create new table with updated schema
				CREATE TABLE file_metadata_new (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					mapping_id TEXT NOT NULL,
					object_name TEXT NOT NULL,
					size INTEGER NOT NULL,
					last_modified TIMESTAMP NOT NULL,
					etag TEXT,
					content_type TEXT,
					last_synced TIMESTAMP NOT NULL,
					sync_status TEXT NOT NULL,
					UNIQUE(mapping_id, object_name)
				);
				
				-- Copy data from old table, generating mapping_id from bucket_name
				INSERT OR IGNORE INTO file_metadata_new (
					mapping_id, object_name, size, last_modified, etag, 
					content_type, last_synced, sync_status
				)
				SELECT 
					'default:' || bucket_name || '->default:' || bucket_name as mapping_id,
					object_name, size, last_modified, etag, 
					content_type, last_synced, sync_status
				FROM file_metadata;
				
				-- Drop old table
				DROP TABLE file_metadata;
				
				-- Rename new table to original name
				ALTER TABLE file_metadata_new RENAME TO file_metadata;
				
				-- Create new index
				CREATE INDEX idx_file_metadata_mapping_object 
				ON file_metadata(mapping_id, object_name);
			`)

			return err
		}

		if hasSourceBucket {
			_, err = tx.Exec(`
				-- Create new table with updated schema
				CREATE TABLE file_metadata_new (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					mapping_id TEXT NOT NULL,
					object_name TEXT NOT NULL,
					size INTEGER NOT NULL,
					last_modified TIMESTAMP NOT NULL,
					etag TEXT,
					content_type TEXT,
					last_synced TIMESTAMP NOT NULL,
					sync_status TEXT NOT NULL,
					UNIQUE(mapping_id, object_name)
				);
				
				-- Copy data from old table, generating mapping_id from source and target
				INSERT OR IGNORE INTO file_metadata_new (
					mapping_id, object_name, size, last_modified, etag, 
					content_type, last_synced, sync_status
				)
				SELECT 
					'default:' || source_bucket || '->default:' || target_bucket as mapping_id,
					object_name, size, last_modified, etag, 
					content_type, last_synced, sync_status
				FROM file_metadata;
				
				-- Drop old table
				DROP TABLE file_metadata;
				
				-- Rename new table to original name
				ALTER TABLE file_metadata_new RENAME TO file_metadata;
				
				-- Create new index
				CREATE INDEX idx_file_metadata_mapping_object 
				ON file_metadata(mapping_id, object_name);
			`)

			return err
		}
	}

	if err != nil {
		return fmt.Errorf("error applying migration %d: %v", version, err)
	}

	return nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) GetFileMetadata(mappingID, objectName string) (*FileMetadata, error) {
	var metadata FileMetadata
	err := db.db.QueryRow(`
		SELECT id, mapping_id, object_name, size, last_modified, etag, content_type, last_synced, sync_status
		FROM file_metadata
		WHERE mapping_id = ? AND object_name = ?
	`, mappingID, objectName).Scan(
		&metadata.ID,
		&metadata.MappingID,
		&metadata.ObjectName,
		&metadata.Size,
		&metadata.LastModified,
		&metadata.ETag,
		&metadata.ContentType,
		&metadata.LastSynced,
		&metadata.SyncStatus,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error querying metadata: %v", err)
	}
	return &metadata, nil
}

func (db *DB) UpsertFileMetadata(metadata *FileMetadata) error {
	_, err := db.db.Exec(`
		INSERT INTO file_metadata 
		(mapping_id, object_name, size, last_modified, etag, content_type, last_synced, sync_status) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(mapping_id, object_name) DO UPDATE SET
		size = ?, last_modified = ?, etag = ?, content_type = ?, last_synced = ?, sync_status = ?
	`,
		metadata.MappingID, metadata.ObjectName, metadata.Size, metadata.LastModified,
		metadata.ETag, metadata.ContentType, metadata.LastSynced, metadata.SyncStatus,
		metadata.Size, metadata.LastModified, metadata.ETag, metadata.ContentType,
		metadata.LastSynced, metadata.SyncStatus,
	)

	if err != nil {
		return fmt.Errorf("error inserting/updating metadata: %v", err)
	}
	return nil
}

func (db *DB) ListFileMetadataByMapping(mappingID string) ([]*FileMetadata, error) {
	rows, err := db.db.Query(`
		SELECT id, mapping_id, object_name, size, last_modified, etag, content_type, last_synced, sync_status
		FROM file_metadata
		WHERE mapping_id = ?
	`, mappingID)

	if err != nil {
		return nil, fmt.Errorf("error listing metadata: %v", err)
	}
	defer rows.Close()

	var files []*FileMetadata
	for rows.Next() {
		var meta FileMetadata
		err := rows.Scan(
			&meta.ID,
			&meta.MappingID,
			&meta.ObjectName,
			&meta.Size,
			&meta.LastModified,
			&meta.ETag,
			&meta.ContentType,
			&meta.LastSynced,
			&meta.SyncStatus,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning metadata: %v", err)
		}
		files = append(files, &meta)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating results: %v", err)
	}

	return files, nil
}

func (db *DB) DeleteFileMetadata(mappingID, objectName string) error {
	_, err := db.db.Exec(`
		DELETE FROM file_metadata
		WHERE mapping_id = ? AND object_name = ?
	`, mappingID, objectName)

	if err != nil {
		return fmt.Errorf("error deleting metadata: %v", err)
	}
	return nil
}
