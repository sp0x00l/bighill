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

type InferenceModel struct {
	ModelID           uuid.UUID
	UserID            uuid.UUID
	OrgID             uuid.UUID
	TrainingRunID     uuid.UUID
	DatasetID         uuid.UUID
	ModelKind         ModelKind
	Source            ModelSource
	SourceURI         string
	SourceMetadata    string
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

func (m *InferenceModel) RequiresDatasetMatch() bool {
	return m.ModelKind.String() != ModelKindBase.String()
}
