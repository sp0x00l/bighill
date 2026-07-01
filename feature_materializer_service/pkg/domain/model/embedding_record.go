package model

import "github.com/google/uuid"

type EmbeddingRecord struct {
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	ChunkIndex          int
	SourceText          string
	Vector              []float32
}
