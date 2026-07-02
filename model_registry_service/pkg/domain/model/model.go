package model

import "github.com/google/uuid"

type Model struct {
	ModelID           uuid.UUID
	TrainingRunID     uuid.UUID
	DatasetID         uuid.UUID
	Name              string
	ModelVersion      int
	BaseModel         string
	ArtifactLocation  string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	MetricsMetadata   string
	Status            ModelStatus
	FailureReason     string
}
