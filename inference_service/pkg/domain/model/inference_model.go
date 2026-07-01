package model

import (
	"fmt"

	"github.com/google/uuid"
)

type ModelStatus int

const (
	ModelStatusPending ModelStatus = iota
	ModelStatusReady
	ModelStatusFailed
)

func (s ModelStatus) String() string {
	if s < ModelStatusPending || s > ModelStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"PENDING", "READY", "FAILED"}[s]
}

func ToModelStatus(value string) (ModelStatus, error) {
	switch value {
	case "PENDING":
		return ModelStatusPending, nil
	case "READY":
		return ModelStatusReady, nil
	case "FAILED":
		return ModelStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model status %q", value)
	}
}

type InferenceModel struct {
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
