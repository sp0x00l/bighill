package model

import (
	"fmt"

	"github.com/google/uuid"
)

type ModelStatus int

const (
	ModelStatusPending ModelStatus = iota
	ModelStatusCandidate
	ModelStatusEvaluated
	ModelStatusReady
	ModelStatusFailed
)

func (s ModelStatus) String() string {
	if s < ModelStatusPending || s > ModelStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"PENDING", "CANDIDATE", "EVALUATED", "READY", "FAILED"}[s]
}

func ToModelStatus(value string) (ModelStatus, error) {
	switch value {
	case "PENDING":
		return ModelStatusPending, nil
	case "CANDIDATE":
		return ModelStatusCandidate, nil
	case "EVALUATED":
		return ModelStatusEvaluated, nil
	case "READY":
		return ModelStatusReady, nil
	case "FAILED":
		return ModelStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model status %q", value)
	}
}

type ModelLoadStatus int

const (
	ModelLoadStatusNotLoaded ModelLoadStatus = iota
	ModelLoadStatusLoaded
	ModelLoadStatusFailed
)

func (s ModelLoadStatus) String() string {
	if s < ModelLoadStatusNotLoaded || s > ModelLoadStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"NOT_LOADED", "LOADED", "FAILED"}[s]
}

func ToModelLoadStatus(value string) (ModelLoadStatus, error) {
	switch value {
	case "", "NOT_LOADED":
		return ModelLoadStatusNotLoaded, nil
	case "LOADED":
		return ModelLoadStatusLoaded, nil
	case "FAILED":
		return ModelLoadStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model load status %q", value)
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
	AdapterURI        string
	ServingTarget     string
	ServingModel      string
	ServingLoadStatus ModelLoadStatus
	MetricsMetadata   string
	Status            ModelStatus
	FailureReason     string
}
