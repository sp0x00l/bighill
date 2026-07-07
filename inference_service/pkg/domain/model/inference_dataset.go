package model

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type DatasetProcessingState int

const (
	DatasetProcessingPending DatasetProcessingState = iota
	DatasetProcessingRawMaterialized
	DatasetProcessingFeatureMaterialized
	DatasetProcessingEmbeddingsMaterialized
	DatasetProcessingFailed
)

func (s DatasetProcessingState) String() string {
	if s < DatasetProcessingPending || s > DatasetProcessingFailed {
		return "UNKNOWN"
	}
	return [...]string{"PENDING", "RAW_MATERIALIZED", "FEATURE_MATERIALIZED", "EMBEDDINGS_MATERIALIZED", "FAILED"}[s]
}

func ToDatasetProcessingState(value string) (DatasetProcessingState, error) {
	switch value {
	case "PENDING":
		return DatasetProcessingPending, nil
	case "RAW_MATERIALIZED":
		return DatasetProcessingRawMaterialized, nil
	case "FEATURE_MATERIALIZED":
		return DatasetProcessingFeatureMaterialized, nil
	case "EMBEDDINGS_MATERIALIZED":
		return DatasetProcessingEmbeddingsMaterialized, nil
	case "FAILED":
		return DatasetProcessingFailed, nil
	default:
		return 0, fmt.Errorf("invalid dataset processing state %q", value)
	}
}

type InferenceDataset struct {
	DatasetID                uuid.UUID
	UserID                   uuid.UUID
	OrgID                    uuid.UUID
	DatasetVersion           int
	ProcessingState          DatasetProcessingState
	StorageLocation          string
	TableNamespace           string
	TableName                string
	TableFormat              string
	CatalogProvider          string
	ProcessingProfile        string
	SchemaVersion            int
	SchemaMetadata           string
	RawSnapshotID            uuid.UUID
	FeatureSnapshotID        uuid.UUID
	EmbeddingSnapshotID      uuid.UUID
	VectorStore              string
	CollectionName           string
	EmbeddingDimensions      int
	EmbeddingCount           int64
	EmbeddingStrategyVersion string
	EmbeddingChunkerName     string
	EmbeddingChunkerVersion  string
	EmbeddingChunkSize       int
	EmbeddingChunkOverlap    int
	EmbeddingProvider        string
	EmbeddingModel           string
}

func (d *InferenceDataset) IsRAGReady() bool {
	if d == nil {
		return false
	}
	return d.ProcessingState == DatasetProcessingEmbeddingsMaterialized &&
		d.EmbeddingSnapshotID != uuid.Nil &&
		d.EmbeddingDimensions > 0 &&
		d.EmbeddingCount > 0
}

func (d *InferenceDataset) HasSupportedEmbeddingProvider() bool {
	if d == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(d.EmbeddingProvider)) {
	case "ollama", "tei":
		return true
	default:
		return false
	}
}
