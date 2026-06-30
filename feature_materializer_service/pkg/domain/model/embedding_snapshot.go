package model

import "github.com/google/uuid"

type EmbeddingSnapshot struct {
	EmbeddingSnapshotID uuid.UUID
	FeatureSnapshotID   uuid.UUID
	DatasetID           uuid.UUID
	VectorStore         string
	CollectionName      string
	Status              SnapshotStatus
	FailureReason       string
}
