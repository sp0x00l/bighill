package model

import "github.com/google/uuid"

type EmbeddingSnapshot struct {
	EmbeddingSnapshotID uuid.UUID
	FeatureSnapshotID   uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	VectorStore         string
	CollectionName      string
	EmbeddingDimensions int
	EmbeddingCount      int64
	StrategyVersion     string
	ChunkerName         string
	ChunkerVersion      string
	ChunkSize           int
	ChunkOverlap        int
	EmbeddingProvider   string
	EmbeddingModel      string
	Status              SnapshotStatus
	FailureReason       string
}
