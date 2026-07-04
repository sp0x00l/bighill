package model

import "github.com/google/uuid"

type EmbeddingRecord struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	ChunkIndex          int
	SourceText          string
	Vector              []float32
	Distance            float64
	Similarity          float64
}
