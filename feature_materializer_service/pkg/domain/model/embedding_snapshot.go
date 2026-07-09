package model

import "github.com/google/uuid"

type EmbeddingSnapshot struct {
	EmbeddingSnapshotID     uuid.UUID
	FeatureSnapshotID       uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	VectorStore             string
	CollectionName          string
	EmbeddingDimensions     int
	EmbeddingCount          int64
	StrategyVersion         string
	ExtractorName           string
	ExtractorVersion        string
	CleanerName             string
	CleanerVersion          string
	ChunkerName             string
	ChunkerVersion          string
	ChunkSize               int
	ChunkOverlap            int
	EmbeddingProvider       string
	EmbeddingModel          string
	ActiveForRetrieval      bool
	Status                  SnapshotStatus
	FailureReason           string
}
