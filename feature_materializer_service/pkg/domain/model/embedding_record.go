package model

import "github.com/google/uuid"

type EmbeddingRecord struct {
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	SourceText          string
	Vector              []float32
}
