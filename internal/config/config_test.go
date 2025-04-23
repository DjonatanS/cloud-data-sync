package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateConfig_Success(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{
			{ID: "p1", Type: GCS, GCS: &GCSConfig{ProjectID: "proj"}},
		},
		Mappings: []BucketMapping{
			{SourceProviderID: "p1", SourceBucket: "sb", TargetProviderID: "p1", TargetBucket: "tb"},
		},
	}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateConfig_NoProviders(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{},
		Mappings:  []BucketMapping{{}},
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error for no providers, got nil")
	}
}

func TestValidateConfig_DuplicateProviderIDs(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{
			{ID: "p1", Type: GCS, GCS: &GCSConfig{ProjectID: "proj"}},
			{ID: "p1", Type: GCS, GCS: &GCSConfig{ProjectID: "proj2"}},
		},
		Mappings: []BucketMapping{{SourceProviderID: "p1", SourceBucket: "sb", TargetProviderID: "p1", TargetBucket: "tb"}},
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error for duplicate provider IDs, got nil")
	}
}

func TestValidateConfig_MissingProviderConfig(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{{ID: "p1", Type: AWS}},
		Mappings:  []BucketMapping{{SourceProviderID: "p1", SourceBucket: "sb", TargetProviderID: "p1", TargetBucket: "tb"}},
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error for missing AWS config, got nil")
	}
}

func TestValidateConfig_NoMappings(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{{ID: "p1", Type: GCS, GCS: &GCSConfig{ProjectID: "proj"}}},
		Mappings:  []BucketMapping{},
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error for no mappings, got nil")
	}
}

func TestValidateConfig_InvalidMappingProvider(t *testing.T) {
	cfg := &Config{
		Providers: []ProviderConfig{{ID: "p1", Type: GCS, GCS: &GCSConfig{ProjectID: "proj"}}},
		Mappings:  []BucketMapping{{SourceProviderID: "p2", SourceBucket: "sb", TargetProviderID: "p1", TargetBucket: "tb"}},
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected error for invalid mapping provider, got nil")
	}
}

func TestLoadConfig_DefaultDBPath(t *testing.T) {
	// Create temporary config file without databasePath
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "testconfig.json")
	json := `{
		"providers": [
			{"id": "p1", "type": "gcs", "gcs": {"projectId": "proj"}}
		],
		"mappings": [
			{"sourceProviderId": "p1", "sourceBucket": "sb", "targetProviderId": "p1", "targetBucket": "tb"}
		]
	}`
	if err := os.WriteFile(cfgPath, []byte(json), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}
	if cfg.DatabasePath != "data.db" {
		t.Fatalf("expected default DatabasePath 'data.db', got %s", cfg.DatabasePath)
	}
}
