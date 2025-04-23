package azure

import (
	"testing"

	"github.com/DjonatanS/cloud-data-sync/internal/interfaces"
)

func TestClient_ImplementsStorageProvider(t *testing.T) {
	var _ interfaces.StorageProvider = (*Client)(nil)
}
